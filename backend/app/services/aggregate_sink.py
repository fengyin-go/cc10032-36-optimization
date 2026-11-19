"""聚合落库领域服务：将实时聚合结果按定时频率批量写入目标存储表。

增强能力: 聚合粒度校验、落库频率管理、分区键生成、覆盖/追加模式、与检查点对齐

批次语义: 批量落库走输出批次台账，每个逻辑分区只维护一条活动记录；
重试替换失败状态，封口后不再追加，完整性报告只统计最新成功与被放弃分区。
"""
from __future__ import annotations

import time
from typing import Any

from app.output_batches import batch_ledger
from app.services.base import BaseModuleService
from app.store import store


class AggregateSinkService(BaseModuleService):
    MODULE = "aggregate_sink"
    ENTITY = "聚合任务"
    LIST_FIELDS = ["任务名称", "源指标集", "聚合粒度", "输出表名", "落库频率", "覆盖模式", "分区键"]
    REQUIRED_FIELDS = ["任务名称", "源指标集", "聚合粒度"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "写入失败"]
    ACTION_RULES = {"启动任务": "待启动", "停止任务": "运行中"}
    NEGATIVE_ACTIONS = []

    def validate_sink_task(self, task_name: str) -> dict[str, Any]:
        """校验聚合落库任务的配置完整性。"""
        task = None
        for r in store.rows(self.MODULE):
            if r.get("任务名称") == task_name:
                task = r
                break
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}

        checks = []
        granularity = task.get("聚合粒度", "")
        valid_granularities = {"minute", "hour", "day", "week", "month"}
        checks.append({
            "check": "granularity",
            "ok": granularity.lower() in valid_granularities,
            "detail": granularity,
        })

        try:
            frequency = int(task.get("落库频率", "60"))
            checks.append({
                "check": "frequency",
                "ok": frequency >= 10 and frequency <= 86400,
                "detail": f"{frequency}s",
            })
        except (ValueError, TypeError):
            checks.append({"check": "frequency", "ok": False, "detail": "格式错误"})

        partition_key = task.get("分区键", "")
        checks.append({
            "check": "partition_key",
            "ok": bool(partition_key and partition_key.strip()),
            "detail": partition_key,
        })

        mode = task.get("覆盖模式", "append")
        checks.append({
            "check": "mode",
            "ok": mode in ("append", "overwrite", "merge"),
            "detail": mode,
        })

        return {
            "ok": all(c["ok"] for c in checks),
            "task": task_name,
            "checks": checks,
        }

    def batch_flush_to_target(self, task_name: str, aggregated: list[dict[str, Any]],
                              version: str | None = None,
                              request_id: str | None = None,
                              outcomes: dict[str, bool] | None = None) -> dict[str, Any]:
        """将聚合结果批量写入目标存储表并同步检查点。

        这是「聚合落库持久化路径」：同一任务+版本+分区集合重复调用返回同一批次，
        一个分区只有一条活动落库记录，重试只替换状态。
        """
        task = None
        for r in store.rows(self.MODULE):
            if r.get("任务名称") == task_name:
                task = r
                break
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}

        partition_key = task.get("分区键", "dt")
        mode = task.get("覆盖模式", "append") or "append"
        batch_version = version or f"agg_{int(time.time() * 1000)}"
        partition_payloads: dict[str, Any] = {}
        for index, item in enumerate(aggregated):
            name = str(item.get("partition") or f"part_{index:02d}")
            partition_payloads[name] = item

        opening = batch_ledger.open_batch(
            task_name=task_name,
            sink_name=f"agg_{task_name}",
            partitions=partition_payloads,
            version=batch_version,
            source="aggregate_sink",
            mode=mode,
            partition_key=partition_key,
        )
        if not opening.get("ok"):
            return opening
        batch_id = opening["batch"]["batch_id"]
        batch = opening["batch"]
        if batch["sealed"]:
            return {
                "ok": True,
                "task": task_name,
                "batch_id": batch_id,
                "written": batch["success"],
                "checkpoint_id": None,
                "already_sealed": True,
                "report": batch_ledger.integrity_report(batch_id)["report"],
            }

        attempt = batch_ledger.attempt(batch_id, request_id=request_id, outcomes=outcomes)
        if not attempt.get("ok"):
            return attempt

        # 同步检查点时只记录批次口径的写入量，便于后续闸门判断。
        checkpoint_ids = [
            int(r.get("id")) for r in store.rows("checkpoint_mgr")
            if str(r.get("id", "")).isdigit()
        ]
        cp_entry = {
            "id": max(checkpoint_ids, default=0) + 1,
            "配置名称": f"checkpoint_{task_name}_{batch_id}",
            "间隔时间": task.get("落库频率", "60"),
            "超时时长": "120",
            "最大并发": "3",
            "对齐策略": "exactly_once",
            "保留数量": 5,
            "外部化路径": f"/data/checkpoints/{task_name}",
            "status": "已启用",
            "checkpoint_id": f"cp_{batch_id}",
            "batch_id": batch_id,
            "report_version": attempt["report"]["report_version"],
            "source_version": batch_version,
            "operator_states": {
                "task": task_name,
                "written": len(attempt["success"]),
                "batch_status": attempt["batch"]["status"],
            },
            "snapshot_ts": time.time(),
        }
        store.rows("checkpoint_mgr").append(cp_entry)

        if attempt["failed"]:
            task["status"] = "写入失败"
            task["pending"] = True
            task["abnormal"] = True
        else:
            task["status"] = "运行中"
            task["pending"] = True

        # 没有失败分区时直接封口，批次不会长期停在中间态。
        final = attempt
        if not attempt["failed"]:
            sealed = batch_ledger.seal(batch_id)
            if sealed.get("ok"):
                final = sealed
                if sealed["report"]["complete"]:
                    task["status"] = "已完成"
                    task["pending"] = False
                    task["abnormal"] = False
                for row in store.rows("checkpoint_mgr"):
                    if row.get("checkpoint_id") == cp_entry["checkpoint_id"]:
                        row["report_version"] = sealed["report"]["report_version"]
                        row["operator_states"]["batch_status"] = sealed["batch"]["status"]
                        break

        report = final.get("report") or attempt["report"]
        return {
            "ok": True,
            "task": task_name,
            "batch_id": batch_id,
            "written": len(attempt["success"]),
            "failed": len(attempt["failed"]),
            "checkpoint_id": cp_entry["checkpoint_id"],
            "sealed": bool(final.get("report", {}).get("sealed")),
            "receipt": final.get("receipt"),
            "report": report,
        }

    def compute_storage_metrics(self) -> dict[str, Any]:
        """计算存储指标：按批次口径统计每个聚合任务的分区记录数。

        重试替换原记录，所以这里的数量是逻辑分区数而不是尝试次数。
        """
        tasks = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        metrics = {}
        total_records = 0
        for task in tasks:
            name = task.get("任务名称", "")
            count = sum(
                1 for s in store.rows("sink_output")
                if s.get("_batch_source") == "aggregate_sink"
                and s.get("_batch_partition")
                and str(s.get("sink", "")) == f"agg_{name}"
            )
            total_records += count
            metrics[name] = count

        return {
            "ok": True,
            "task_count": len(tasks),
            "total_records": total_records,
            "per_task": metrics,
            "avg_per_task": total_records / max(len(tasks), 1),
        }

    def list_sink_status(self) -> list[dict[str, Any]]:
        """列出所有聚合落库任务的状态、关联检查点与最近批次封口状态。"""
        tasks = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        result = []
        for t in tasks:
            name = t.get("任务名称", "")
            checkpoints = [
                c for c in store.rows("checkpoint_mgr")
                if c.get("batch_id")
                and c.get("配置名称", "").startswith(f"checkpoint_{name}_")
            ]
            batches = batch_ledger.list_batches(task_name=name)
            result.append({
                "task": name,
                "granularity": t.get("聚合粒度", ""),
                "mode": t.get("覆盖模式", ""),
                "status": t.get("status", ""),
                "checkpoints": len(checkpoints),
                "batches": len(batches),
                "latest_batch": batches[0]["batch_id"] if batches else None,
                "latest_batch_status": batches[0]["status"] if batches else None,
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["completed"] = sum(1 for r in rows if r.get("status") == "已完成")
        base["failed"] = sum(1 for r in rows if r.get("status") == "写入失败")
        return base