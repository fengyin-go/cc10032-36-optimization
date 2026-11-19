"""检查点领域服务：管理流式任务的检查点策略。

数据流角色: replay_engine → checkpoint_mgr（消费者+生产者）→ restore_plan（消费者）
增强能力: 检查点创建/快照/恢复/对齐、状态校验、外部化存储路径管理、多检查点一致性验证、
批次封口闸门（读取封口后的聚合落库数据与完整性报告来源版本）
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.output_batches import batch_ledger
from app.services.base import BaseModuleService
from app.store import store


class CheckpointMgrService(BaseModuleService):
    MODULE = "checkpoint_mgr"
    ENTITY = "检查点配置"
    LIST_FIELDS = ["配置名称", "间隔时间", "超时时长", "最大并发", "对齐策略", "保留数量", "外部化路径"]
    REQUIRED_FIELDS = ["配置名称", "间隔时间", "超时时长"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用检查点": "草稿", "停用检查点": "已启用"}
    NEGATIVE_ACTIONS = ["停用检查点"]

    # ---- 跨模块数据管理 ----

    def create_checkpoint(
        self, config_name: str, operator_states: dict[str, Any],
    ) -> dict[str, Any]:
        """创建算子状态检查点快照。

        为指定的检查点配置生成带有状态快照的检查点记录，
        存储到 store 中供 replay_engine 消费。
        """
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}
        if config.get("status") != "已启用":
            return {"ok": False, "error": f"配置「{config_name}」未启用"}

        try:
            interval = float(config.get("间隔时间", "30"))
        except (ValueError, TypeError):
            interval = 30.0

        cp_id = hashlib.md5(
            f"{config_name}_{time.time()}".encode()
        ).hexdigest()[:12]

        snapshot = {
            "id": cp_id,
            "配置名称": config_name,
            "间隔时间": str(interval),
            "超时时长": config.get("超时时长", "60"),
            "最大并发": config.get("最大并发", "3"),
            "对齐策略": config.get("对齐策略", "exactly_once"),
            "保留数量": int(config.get("保留数量", 5)),
            "外部化路径": config.get("外部化路径", "/data/checkpoints"),
            "status": "已启用",
            "pending": False,
            "abnormal": False,
            "checkpoint_id": cp_id,
            "operator_states": copy.deepcopy(operator_states),
            "snapshot_ts": time.time(),
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(snapshot)

        # 清理旧检查点（保留最近 N 个）
        self._prune_old_checkpoints(config_name)

        return {
            "ok": True,
            "checkpoint_id": cp_id,
            "config": config_name,
            "operators": list(operator_states.keys()),
            "interval": interval,
        }

    def _prune_old_checkpoints(self, config_name: str) -> None:
        """清理超过保留数量的旧检查点。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        max_keep = int((config or {}).get("保留数量", 5))

        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0), reverse=True)
        for s in snapshots[max_keep:]:
            s["status"] = "已停用"
            s["_pruned"] = True

    def restore_state(self, checkpoint_id: str) -> dict[str, Any]:
        """从检查点恢复算子状态，验证完整性后返回。"""
        snapshot = None
        for r in store.rows(self.MODULE):
            if r.get("checkpoint_id") == checkpoint_id:
                snapshot = r
                break
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}

        states = snapshot.get("operator_states", {})
        # 验证状态完整性
        validation = self._validate_state_structure(states)

        return {
            "ok": validation["valid"],
            "checkpoint_id": checkpoint_id,
            "config": snapshot.get("配置名称", ""),
            "snapshot_ts": snapshot.get("snapshot_ts", 0),
            "operator_count": len(states),
            "operators": list(states.keys()),
            "states": states,
            "validation": validation,
        }

    @staticmethod
    def _validate_state_structure(states: dict[str, Any]) -> dict[str, Any]:
        """验证状态数据结构完整性。"""
        issues = []
        if not isinstance(states, dict):
            return {"valid": False, "issues": ["状态结构不是 dict 类型"]}
        for op_name, op_state in states.items():
            if op_state is None:
                issues.append(f"算子「{op_name}」状态为 None")
            elif isinstance(op_state, dict) and len(op_state) == 0:
                issues.append(f"算子「{op_name}」状态为空字典")
        return {"valid": len(issues) == 0, "issues": issues}

    def verify_multi_checkpoint_consistency(
        self, config_name: str,
    ) -> dict[str, Any]:
        """验证同一配置下多个检查点之间的状态一致性。"""
        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0))

        if len(snapshots) < 2:
            return {"ok": True, "consistent": True, "message": "检查点不足2个，无需比较"}

        baseline = snapshots[0].get("operator_states", {})
        inconsistencies = []
        for i, s in enumerate(snapshots[1:], 1):
            current = s.get("operator_states", {})
            for op, state in current.items():
                if op not in baseline:
                    inconsistencies.append(f"检查点{i}: 新增算子「{op}」")
                elif state != baseline.get(op):
                    inconsistencies.append(f"检查点{i}: 算子「{op}」状态变更")

        return {
            "ok": True,
            "config": config_name,
            "snapshot_count": len(snapshots),
            "consistent": len(inconsistencies) == 0,
            "inconsistencies": inconsistencies,
        }

    def list_checkpoints_for_replay(self) -> list[dict[str, Any]]:
        """列出可用于回放引擎消费的检查点。"""
        available = [
            r for r in store.rows(self.MODULE)
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        ]
        result = []
        for cp in available:
            states = cp.get("operator_states", {})
            result.append({
                "checkpoint_id": cp.get("checkpoint_id"),
                "config": cp.get("配置名称", ""),
                "operator_count": len(states),
                "operators": list(states.keys()),
                "snapshot_ts": cp.get("snapshot_ts", 0),
                "external_path": cp.get("外部化路径", ""),
            })
        return result

    # ---- 输出批次封口闸门 ----

    def verify_batch_gate(self, batch_id: str, version: str | None = None) -> dict[str, Any]:
        """读取封口后的聚合落库数据，确认批次无活动失败项后才允许下一批复用同一版本。

        闸门同时返回完整性报告的来源版本与报告版本，供调用方对照批次口径。
        """
        gate = batch_ledger.checkpoint_gate(batch_id, version)
        report = gate.get("report", {})
        persisted = [
            row.get("source_version")
            for row in store.rows(self.MODULE)
            if row.get("batch_id") == batch_id
        ]
        return {
            **gate,
            "report_source_version": report.get("source_version"),
            "report_version_persisted": report.get("report_version"),
            "checkpoint_versions": persisted,
        }

    def create_checkpoint_for_batch(
        self, config_name: str, batch_id: str, operator_states: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """为一个已封口且无活动失败项的批次创建检查点快照。

        未封口的批次不允许生成检查点，避免下一批在中间态上继续写同一版本。
        """
        gate = batch_ledger.checkpoint_gate(batch_id)
        if not gate.get("allowed"):
            return {
                "ok": False,
                "error": "批次未通过封口闸门：" + "；".join(gate.get("reasons", [])),
                "gate": gate,
            }
        report = gate["report"]
        states = operator_states or {
            "batch": {
                "batch_id": batch_id,
                "source_version": report["source_version"],
                "report_version": report["report_version"],
                "complete": report["complete"],
                "abandoned_partitions": report["abandoned_partitions"],
            }
        }
        result = self.create_checkpoint(config_name, states)
        if result.get("ok"):
            for row in store.rows(self.MODULE):
                if row.get("checkpoint_id") == result["checkpoint_id"]:
                    row["batch_id"] = batch_id
                    row["source_version"] = report["source_version"]
                    row["report_version"] = report["report_version"]
                    break
            result["gate"] = gate
        return result

    def list_batch_reports(self, task_name: str | None = None,
                           source: str | None = None) -> list[dict[str, Any]]:
        """列出已封口的输出完整性报告，保留来源版本。"""
        return batch_ledger.list_reports(task_name=task_name, source=source)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_snapshots"] = sum(1 for r in rows if r.get("checkpoint_id"))
        base["available_snapshots"] = sum(
            1 for r in rows
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        )
        base["pruned_snapshots"] = sum(1 for r in rows if r.get("_pruned"))
        base["sealed_batch_snapshots"] = sum(
            1 for r in rows
            if r.get("batch_id") and r.get("status") == "已启用"
        )
        return base