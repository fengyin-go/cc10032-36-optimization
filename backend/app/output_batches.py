"""输出批次台账：把「聚合落库 → 结果落地 → 完整性报告 → 检查点」串成一条可封口的链路。

设计要点：
  * 一个批次内，每个逻辑分区只保留一条活动落库记录；重试是「替换」失败状态，
    不是追加新记录，所以页面统计总量不会随重试次数增长。
  * 批次一旦封口（sealed）就拒绝任何追加写入，避免落库数据与完整性报告长期停在中间态。
  * 完整性报告只统计「每个分区的最新成功结果」和「明确放弃的分区」，
    活动失败项单独列出且不计入成功数。
  * 封口时生成完成回执，检查点模块读取封口后的批次与报告版本，
    确认没有活动失败项后才允许下一批复用同一版本。
  * 所有写操作在一把可重入锁内完成；同一 request_id 的并发重试只生效一次，
    事务中途异常会回滚到操作前的版本。
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from typing import Any

from app.store import store

BATCH_COLLECTING = "collecting"
BATCH_SEALED_COMPLETE = "sealed_complete"
BATCH_SEALED_GAP = "sealed_with_gap"

PARTITION_PENDING = "pending"
PARTITION_SUCCESS = "success"
PARTITION_FAILED = "failed"
PARTITION_ABANDONED = "abandoned"


def _batch_id_for(source: str, task_name: str, version: str, sink_name: str,
                  partitions: list[str]) -> str:
    digest = hashlib.md5(
        f"{source}|{task_name}|{version}|{sink_name}|{','.join(sorted(partitions))}".encode()
    ).hexdigest()[:12]
    return f"rb_{digest}"


def _simulate_success(partition: str, attempt_no: int) -> bool:
    """确定性模拟：同一分区在前几次尝试里偶发失败，重试后收敛。"""
    return (hash(f"{partition}:{attempt_no}") % 5) != 0


class OutputBatchLedger:
    """批次台账：批次、分区、落库记录替换、完整性报告和版本闸门。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # batch_id → 批次状态
        self._batches: dict[str, dict[str, Any]] = {}
        # batch_id → 按 report_version 追加的完整性报告（保留不同来源版本）
        self._reports: dict[str, list[dict[str, Any]]] = {}
        # (batch_id, request_id) → 已处理结果，保证重试幂等
        self._requests: dict[tuple[str, str], dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # 批次登记
    # ------------------------------------------------------------------

    def open_batch(
        self,
        *,
        task_name: str,
        sink_name: str,
        partitions: list[str] | dict[str, Any],
        version: str,
        source: str = "sink_output",
        deadline_seconds: float = 300.0,
        request_id: str | None = None,
        mode: str = "overwrite",
        partition_key: str = "batch_partition",
    ) -> dict[str, Any]:
        """登记一个输出批次；同样的来源/版本/分区集合重复登记返回同一批次。"""
        if isinstance(partitions, dict):
            partition_payloads = {str(k): v for k, v in partitions.items()}
        else:
            partition_payloads = {str(p): {"partition": str(p)} for p in partitions}
        partition_names = list(partition_payloads)
        if not partition_names:
            return {"ok": False, "error": "批次至少需要一个逻辑分区", "code": "empty_partitions"}
        if not version:
            return {"ok": False, "error": "批次必须带来源版本", "code": "missing_version"}

        with self._lock:
            batch_id = _batch_id_for(source, task_name, version, sink_name, partition_names)
            existing = self._batches.get(batch_id)
            if existing is not None:
                return {
                    "ok": True,
                    "existing": True,
                    "batch": self._summary(existing),
                    "report": self._build_report(existing),
                }

            now = time.time()
            batch = {
                "batch_id": batch_id,
                "task_name": task_name,
                "sink_name": sink_name,
                "version": version,
                "source": source,
                "mode": mode,
                "partition_key": partition_key,
                "status": BATCH_COLLECTING,
                "sealed": False,
                "created_at": now,
                "updated_at": now,
                "deadline_at": now + max(float(deadline_seconds), 0.0),
                "attempts": 0,
                "replaced_count": 0,
                "report_version": 0,
                "receipt": None,
                "sealed_at": None,
                "partitions": {
                    name: {
                        "partition": name,
                        "status": PARTITION_PENDING,
                        "attempts": 0,
                        "record_id": None,
                        "last_error": None,
                        "success_at": None,
                        "abandoned_at": None,
                        "payload": copy.deepcopy(payload),
                    }
                    for name, payload in partition_payloads.items()
                },
                "history": [],
                "replaced_records": [],
            }
            self._batches[batch_id] = batch
            if request_id:
                self._requests[(batch_id, request_id)] = {
                    "ok": True, "batch": self._summary(batch),
                }
            return {
                "ok": True,
                "existing": False,
                "batch": self._summary(batch),
                "report": self._build_report(batch),
            }

    # ------------------------------------------------------------------
    # 失败重试（替换同一逻辑分区的失败状态）
    # ------------------------------------------------------------------

    def attempt(
        self,
        batch_id: str,
        *,
        request_id: str | None = None,
        forced_failures: set[str] | None = None,
        outcomes: dict[str, bool] | None = None,
        extend_seconds: float = 0.0,
    ) -> dict[str, Any]:
        """执行一轮写入/重试：只处理未成功且未放弃的分区，并在原记录上替换状态。"""
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return {"ok": False, "error": f"批次「{batch_id}」不存在", "code": "batch_missing"}
            if request_id and (batch_id, request_id) in self._requests:
                replay = copy.deepcopy(self._requests[(batch_id, request_id)])
                replay["idempotent_replay"] = True
                return replay
            if batch["sealed"]:
                return {
                    "ok": False,
                    "error": "批次已封口，禁止追加或重试写入",
                    "code": "batch_sealed",
                    "batch": self._summary(batch),
                }

            targets = [
                name for name, state in batch["partitions"].items()
                if state["status"] in (PARTITION_PENDING, PARTITION_FAILED)
            ]
            if not targets:
                return {
                    "ok": True,
                    "batch": self._summary(batch),
                    "attempt": batch["attempts"],
                    "processed": 0,
                    "success": [],
                    "failed": [],
                    "replaced": 0,
                    "report": self._build_report(batch),
                    "message": "没有待重试的分区",
                }
            if extend_seconds:
                batch["deadline_at"] = max(batch["deadline_at"], time.time()) + extend_seconds

            before_batch = copy.deepcopy(batch)
            before_rows = copy.deepcopy(store.rows("sink_output"))
            try:
                result = self._apply_attempt(
                    batch, targets, outcomes=outcomes, forced_failures=forced_failures,
                )
            except Exception as exc:  # 事务失败：回滚批次与落库记录，保持原版本
                self._batches[batch_id] = before_batch
                store.rows("sink_output")[:] = before_rows
                return {
                    "ok": False,
                    "error": f"批次写入事务失败，已回滚到原版本：{exc}",
                    "code": "transaction_failed",
                    "batch": self._summary(self._batches[batch_id]),
                }

            if request_id:
                self._requests[(batch_id, request_id)] = copy.deepcopy(result)
            return result

    def _apply_attempt(
        self,
        batch: dict[str, Any],
        targets: list[str],
        *,
        outcomes: dict[str, bool] | None,
        forced_failures: set[str] | None,
    ) -> dict[str, Any]:
        attempt_no = batch["attempts"] + 1
        batch["attempts"] = attempt_no
        success_partitions: list[str] = []
        failed_partitions: list[str] = []
        replaced_now = 0

        for partition in targets:
            state = batch["partitions"][partition]
            state["attempts"] += 1
            if outcomes is not None and partition in outcomes:
                ok = bool(outcomes[partition])
            elif forced_failures is not None:
                ok = partition not in forced_failures
            else:
                ok = _simulate_success(partition, attempt_no)

            row = self._find_record_row(batch["batch_id"], partition)
            if row is None:
                row = self._new_record(batch, partition, state)
                store.rows("sink_output").append(row)
            else:
                previous = row.get("write_status")
                if previous == PARTITION_FAILED:
                    batch["replaced_count"] += 1
                    replaced_now += 1
                    batch["replaced_records"].append({
                        "attempt": attempt_no,
                        "partition": partition,
                        "record_id": row.get("id"),
                        "previous_status": previous,
                        "previous_error": row.get("last_error"),
                        "new_status": PARTITION_SUCCESS if ok else PARTITION_FAILED,
                        "replaced_at": time.time(),
                    })

            row["write_status"] = PARTITION_SUCCESS if ok else PARTITION_FAILED
            row["attempts"] = state["attempts"]
            row["updated_at"] = time.time()
            row["status"] = "已生效" if ok else "写入异常"
            row["abnormal"] = not ok
            row["last_error"] = None if ok else f"分区 {partition} 第{state['attempts']}次写入失败"
            if ok:
                row["written_at"] = time.time()
                row["data"] = copy.deepcopy(state["payload"])
                state["status"] = PARTITION_SUCCESS
                state["success_at"] = time.time()
                state["last_error"] = None
                success_partitions.append(partition)
            else:
                state["status"] = PARTITION_FAILED
                state["last_error"] = row["last_error"]
                failed_partitions.append(partition)
            state["record_id"] = row["id"]

        batch["updated_at"] = time.time()
        batch["history"].append({
            "attempt": attempt_no,
            "at": batch["updated_at"],
            "partitions": list(targets),
            "success": success_partitions,
            "failed": failed_partitions,
            "replaced": replaced_now,
        })
        result = {
            "ok": True,
            "batch": self._summary(batch),
            "attempt": attempt_no,
            "processed": len(targets),
            "success": success_partitions,
            "failed": failed_partitions,
            "replaced": replaced_now,
            "report": self._build_report(batch),
        }
        return result

    def _find_record_row(self, batch_id: str, partition: str) -> dict[str, Any] | None:
        for row in store.rows("sink_output"):
            if row.get("_batch_id") == batch_id and row.get("_batch_partition") == partition:
                return row
        return None

    def _new_record(
        self, batch: dict[str, Any], partition: str, state: dict[str, Any],
    ) -> dict[str, Any]:
        rows = store.rows("sink_output")
        record_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        now = time.time()
        return {
            "id": record_id,
            "配置名称": batch["sink_name"],
            "输出类型": "aggregate_batch",
            "目标地址": f"table://{batch['task_name']}",
            "写入模式": batch["mode"],
            "分区策略": batch["partition_key"],
            "格式转换": "json",
            "批量大小": "",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "sink": batch["sink_name"],
            "batch_id": batch["batch_id"],
            "version": batch["version"],
            "partition": partition,
            "write_status": PARTITION_PENDING,
            "attempts": state["attempts"],
            "written_at": now,
            "updated_at": now,
            "_batch_id": batch["batch_id"],
            "_batch_partition": partition,
            "_batch_source": batch["source"],
        }

    # ------------------------------------------------------------------
    # 封口与完成回执
    # ------------------------------------------------------------------

    def seal(
        self,
        batch_id: str,
        *,
        abandon_failed: bool = False,
        request_id: str | None = None,
        abandon_reason: str = "到达批次截止时间，放弃失败分区并带缺口发布",
    ) -> dict[str, Any]:
        """封口批次：要么全部成功，要么明确放弃失败分区后带缺口发布。"""
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return {"ok": False, "error": f"批次「{batch_id}」不存在", "code": "batch_missing"}
            if request_id and (batch_id, request_id) in self._requests:
                replay = copy.deepcopy(self._requests[(batch_id, request_id)])
                replay["idempotent_replay"] = True
                return replay
            if batch["sealed"]:
                return {
                    "ok": True,
                    "already_sealed": True,
                    "batch": self._summary(batch),
                    "receipt": copy.deepcopy(batch["receipt"]),
                    "report": self._build_report(batch),
                }

            failed = [
                name for name, state in batch["partitions"].items()
                if state["status"] == PARTITION_FAILED
            ]
            if failed and not abandon_failed:
                return {
                    "ok": False,
                    "error": "批次仍有活动失败分区，不能完整封口；可继续重试或放弃失败分区后带缺口封口",
                    "code": "active_failures",
                    "failed_partitions": failed,
                    "past_deadline": time.time() >= batch["deadline_at"],
                    "batch": self._summary(batch),
                }

            abandoned: list[str] = []
            if failed and abandon_failed:
                now = time.time()
                for partition in failed:
                    state = batch["partitions"][partition]
                    state["status"] = PARTITION_ABANDONED
                    state["abandoned_at"] = now
                    state["last_error"] = abandon_reason
                    row = self._find_record_row(batch_id, partition)
                    if row is not None:
                        row["write_status"] = PARTITION_ABANDONED
                        row["status"] = "已停用"
                        row["abandoned"] = True
                        row["updated_at"] = now
                        row["last_error"] = abandon_reason
                    batch["history"].append({
                        "attempt": batch["attempts"],
                        "at": now,
                        "partitions": [partition],
                        "abandoned": [partition],
                        "reason": abandon_reason,
                    })
                    abandoned.append(partition)

            success = sum(
                1 for state in batch["partitions"].values()
                if state["status"] == PARTITION_SUCCESS
            )
            total = len(batch["partitions"])
            batch["status"] = (
                BATCH_SEALED_COMPLETE if success == total else BATCH_SEALED_GAP
            )
            batch["sealed"] = True
            batch["sealed_at"] = time.time()
            batch["updated_at"] = batch["sealed_at"]
            # 每次封口产生一个新的报告版本，并持久保留下来供检查点/刷新后读取。
            batch["report_version"] = int(batch.get("report_version", 0)) + 1

            report = self._build_report(batch, persist=True)
            receipt = {
                "receipt_id": f"rc_{batch_id}_{report['report_version']}",
                "batch_id": batch_id,
                "status": batch["status"],
                "status_label": report["status_label"],
                "complete": report["complete"],
                "source": batch["source"],
                "task_name": batch["task_name"],
                "sink_name": batch["sink_name"],
                "source_version": batch["version"],
                "report_version": report["report_version"],
                "success": report["success"],
                "abandoned": report["abandoned"],
                "active_failures": report["active_failures"],
                "attempts": batch["attempts"],
                "retry_count": report["retry_count"],
                "replaced_count": batch["replaced_count"],
                "abandoned_partitions": abandoned,
                "sealed_at": batch["sealed_at"],
            }
            batch["receipt"] = receipt
            batch["history"].append({
                "attempt": batch["attempts"],
                "at": batch["sealed_at"],
                "sealed": True,
                "status": batch["status"],
                "receipt_id": receipt["receipt_id"],
            })

            result = {
                "ok": True,
                "batch": self._summary(batch),
                "receipt": copy.deepcopy(receipt),
                "report": copy.deepcopy(report),
            }
            if request_id:
                self._requests[(batch_id, request_id)] = copy.deepcopy(result)
            return result

    # ------------------------------------------------------------------
    # 完整性报告
    # ------------------------------------------------------------------

    def _build_report(self, batch: dict[str, Any], persist: bool = False) -> dict[str, Any]:
        """只统计每个分区的最新成功结果与被放弃分区。"""
        states = batch["partitions"].values()
        total = len(batch["partitions"])
        success_states = [s for s in states if s["status"] == PARTITION_SUCCESS]
        abandoned_states = [s for s in states if s["status"] == PARTITION_ABANDONED]
        active_failures = [
            s["partition"] for s in states
            if s["status"] in (PARTITION_FAILED, PARTITION_PENDING)
        ]
        sealed = bool(batch["sealed"])
        complete = sealed and not abandoned_states and not active_failures
        if complete:
            status = BATCH_SEALED_COMPLETE
            label = "批次完整"
        elif sealed:
            status = BATCH_SEALED_GAP
            label = "批次带缺口封口"
        else:
            status = BATCH_COLLECTING
            label = "批次未封口" if not active_failures else "批次进行中"

        report_version = int(batch.get("report_version", 0))
        report = {
            "batch_id": batch["batch_id"],
            "source": batch["source"],
            "source_version": batch["version"],
            "report_version": report_version,
            "task_name": batch["task_name"],
            "sink_name": batch["sink_name"],
            "sealed": sealed,
            "complete": complete,
            "status": status,
            "status_label": label,
            "expected": total,
            "success": len(success_states),
            "abandoned": len(abandoned_states),
            "active_failures": len(active_failures),
            "failed_partitions": active_failures,
            "abandoned_partitions": [s["partition"] for s in abandoned_states],
            "success_partitions": sorted(s["partition"] for s in success_states),
            "attempts": batch["attempts"],
            "retry_count": max(batch["attempts"] - 1, 0),
            "replaced_count": batch["replaced_count"],
            "generated_at": time.time(),
        }
        if persist:
            self._reports.setdefault(batch["batch_id"], []).append(copy.deepcopy(report))
        return report

    def integrity_report(self, batch_id: str, version: str | None = None) -> dict[str, Any]:
        """读取完整性报告；已封口批次返回持久化版本，并可按来源版本筛选。"""
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return {"ok": False, "error": f"批次「{batch_id}」不存在", "code": "batch_missing"}
            reports = self._reports.get(batch_id, [])
            if reports:
                matches = [
                    r for r in reports if version is None or r["source_version"] == version
                ]
                if matches:
                    return {"ok": True, "report": copy.deepcopy(matches[-1]), "history": copy.deepcopy(matches)}
            return {"ok": True, "report": self._build_report(batch)}

    def report_history(self, batch_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._reports.get(batch_id, []))

    def checkpoint_gate(self, batch_id: str, version: str | None = None) -> dict[str, Any]:
        """检查点闸门：只有封口且无活动失败项的批次才允许下一批复用同一版本。"""
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return {"ok": False, "allowed": False, "error": f"批次「{batch_id}」不存在"}
            report = self._build_report(batch)
            reasons: list[str] = []
            if not batch["sealed"]:
                reasons.append("批次尚未封口")
            if report["active_failures"]:
                reasons.append(f"批次仍有 {report['active_failures']} 个活动失败分区")
            if version is not None and version != batch["version"]:
                reasons.append(f"版本不一致：批次为 {batch['version']}，请求为 {version}")
            allowed = not reasons
            return {
                "ok": True,
                "allowed": allowed,
                "batch_id": batch_id,
                "source_version": batch["version"],
                "report_version": report["report_version"],
                "complete": report["complete"],
                "status": report["status"],
                "active_failures": report["active_failures"],
                "abandoned": report["abandoned"],
                "reasons": reasons,
                "report": report,
            }

    def confirm_batch(self, batch_id: str) -> dict[str, Any]:
        """供同步核对使用的轻量回执：封口状态 + 报告版本 + 重试统计。"""
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return {"ok": False, "error": f"批次「{batch_id}」不存在"}
            report = self._build_report(batch)
            return {
                "ok": True,
                "batch": self._summary(batch),
                "receipt": copy.deepcopy(batch["receipt"]),
                "report": report,
                "replaced_records": copy.deepcopy(batch["replaced_records"]),
                "history": copy.deepcopy(batch["history"]),
            }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def _summary(self, batch: dict[str, Any]) -> dict[str, Any]:
        states = list(batch["partitions"].values())
        report = self._build_report(batch)
        return {
            "batch_id": batch["batch_id"],
            "task_name": batch["task_name"],
            "sink_name": batch["sink_name"],
            "source": batch["source"],
            "version": batch["version"],
            "status": batch["status"],
            "sealed": batch["sealed"],
            "complete": report["complete"],
            "status_label": report["status_label"],
            "partition_count": len(states),
            "success": report["success"],
            "failed": report["active_failures"],
            "abandoned": report["abandoned"],
            "attempts": batch["attempts"],
            "retry_count": report["retry_count"],
            "replaced_count": batch["replaced_count"],
            "report_version": batch["report_version"],
            "created_at": batch["created_at"],
            "updated_at": batch["updated_at"],
            "deadline_at": batch["deadline_at"],
            "sealed_at": batch["sealed_at"],
        }

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return None
            return copy.deepcopy(batch)

    def list_batches(
        self,
        *,
        task_name: str | None = None,
        sink_name: str | None = None,
        source: str | None = None,
        status: str | None = None,
        version: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            batches = list(self._batches.values())
            if task_name:
                batches = [b for b in batches if b["task_name"] == task_name]
            if sink_name:
                batches = [b for b in batches if b["sink_name"] == sink_name]
            if source:
                batches = [b for b in batches if b["source"] == source]
            if status:
                batches = [b for b in batches if b["status"] == status]
            if version:
                batches = [b for b in batches if b["version"] == version]
            batches.sort(key=lambda b: b["created_at"], reverse=True)
            return [self._summary(b) for b in batches]

    def list_reports(
        self,
        *,
        task_name: str | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        """按来源版本列出已封口的完整性报告，保留两组持久化路径的不同版本。"""
        with self._lock:
            reports: list[dict[str, Any]] = []
            for batch in self._batches.values():
                if task_name and batch["task_name"] != task_name:
                    continue
                if source and batch["source"] != source:
                    continue
                reports.extend(copy.deepcopy(self._reports.get(batch["batch_id"], [])))
            reports.sort(key=lambda r: (r["generated_at"], r["report_version"]))
            return reports


batch_ledger = OutputBatchLedger()


def seed_demo_batches() -> None:
    """写入两条演示批次：同一份完整性报告口径下保留两条持久化路径的不同版本。

    * aggregate_sink 路径：一次失败重试后完整封口，报告版本 v2026.11.18；
    * sink_output 路径：到达截止时间后放弃一个分区，带缺口封口，报告版本 v2026.11.19。
    """
    if batch_ledger.list_batches(version="v2026.11.18"):
        return

    agg_open = batch_ledger.open_batch(
        task_name="聚合落库样例1",
        sink_name="agg_聚合落库样例1",
        partitions=["dt=20261118/hour=00", "dt=20261118/hour=01",
                    "dt=20261118/hour=02", "dt=20261118/hour=03"],
        version="v2026.11.18",
        source="aggregate_sink",
        mode="overwrite",
        partition_key="dt",
        deadline_seconds=600.0,
    )
    if agg_open.get("ok") and not agg_open.get("batch", {}).get("sealed"):
        agg_id = agg_open["batch"]["batch_id"]
        batch_ledger.attempt(
            agg_id,
            outcomes={
                "dt=20261118/hour=00": True,
                "dt=20261118/hour=01": True,
                "dt=20261118/hour=02": False,
                "dt=20261118/hour=03": True,
            },
        )
        batch_ledger.attempt(
            agg_id,
            outcomes={
                "dt=20261118/hour=00": True,
                "dt=20261118/hour=01": True,
                "dt=20261118/hour=02": True,
                "dt=20261118/hour=03": True,
            },
        )
        batch_ledger.seal(agg_id)

    sink_open = batch_ledger.open_batch(
        task_name="结果落地样例1",
        sink_name="结果落地样例1",
        partitions=["pt_00", "pt_01", "pt_02"],
        version="v2026.11.19",
        source="sink_output",
        mode="append",
        partition_key="round_robin",
        deadline_seconds=0.0,
    )
    if sink_open.get("ok") and not sink_open.get("batch", {}).get("sealed"):
        sink_id = sink_open["batch"]["batch_id"]
        batch_ledger.attempt(
            sink_id,
            outcomes={"pt_00": True, "pt_01": False, "pt_02": True},
        )
        batch_ledger.seal(sink_id, abandon_failed=True)
