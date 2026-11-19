"""结果落地领域服务：管理流式计算结果的输出落地配置。

数据流角色: state_store / calc_operator → sink_output（消费者+生产者）
增强能力: 批量写入调度、分区策略计算、写入模式切换、异常重试、输出完整性校验

批次语义: 每个逻辑分区在 sink_output 中只保留一条活动记录，重试替换同一条记录的
失败状态而不是追加新行；批次封口后拒绝追加，完整性报告只统计最新成功结果与
被放弃分区，完成回执随批次长期保留。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.output_batches import batch_ledger
from app.services.base import BaseModuleService
from app.store import store


class SinkOutputService(BaseModuleService):
    MODULE = "sink_output"
    ENTITY = "输出配置"
    LIST_FIELDS = ["配置名称", "输出类型", "目标地址", "写入模式", "分区策略", "格式转换", "批量大小"]
    REQUIRED_FIELDS = ["配置名称", "输出类型", "目标地址"]
    STATUS_ORDER = ["草稿", "已生效", "已停用", "写入异常"]
    ACTION_RULES = {"启用输出": "草稿", "停用输出": "已生效"}
    NEGATIVE_ACTIONS = ["停用输出"]

    # ---- 跨模块数据消费与生产 ----

    def batch_write_results(
        self, sink_name: str, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """将计算结果批量写入目标存储，支持分区和格式转换。

        走批次台账：同一逻辑分区只保留一条活动记录，失败重试只替换状态。
        """
        sink = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == sink_name:
                sink = r
                break
        if sink is None:
            return {"ok": False, "error": f"输出配置「{sink_name}」不存在"}
        if sink.get("status") == "已停用":
            return {"ok": False, "error": f"输出配置「{sink_name}」已停用"}

        partition_strategy = sink.get("分区策略", "round_robin")
        write_mode = sink.get("写入模式", "append")
        output_type = sink.get("输出类型", "database")
        format_type = sink.get("格式转换", "json")

        partitions: dict[str, Any] = {}
        for i, item in enumerate(results):
            partition_key = self._calc_partition(item, partition_strategy, i)
            partitions[partition_key] = {
                "output_type": output_type,
                "format": format_type,
                "mode": write_mode,
                "data": self._convert_format(item, format_type),
            }

        opening = batch_ledger.open_batch(
            task_name=f"sink_{sink_name}",
            sink_name=sink_name,
            partitions=partitions,
            version=f"batch_{int(time.time() * 1000)}",
            source="sink_output",
            mode=write_mode,
            partition_key=partition_strategy,
        )
        if not opening.get("ok"):
            return opening
        batch_id = opening["batch"]["batch_id"]
        attempt = batch_ledger.attempt(batch_id)
        if not attempt.get("ok"):
            return attempt

        failed = len(attempt["failed"])
        if failed > 0:
            sink["status"] = "写入异常"
            sink["pending"] = True

        return {
            "ok": True,
            "sink": sink_name,
            "batch_id": batch_id,
            "total": len(results),
            "written": len(attempt["success"]),
            "failed": failed,
            "partition_strategy": partition_strategy,
            "report": attempt["report"],
        }

    @staticmethod
    def _calc_partition(
        item: dict[str, Any], strategy: str, index: int,
    ) -> str:
        """计算数据分区键。"""
        if strategy == "hash_key":
            raw = str(item.get("key", index))
            return f"pt_{hashlib.md5(raw.encode()).hexdigest()[:4]}"
        elif strategy == "time_range":
            ts = item.get("timestamp", time.time())
            return time.strftime("dt=%Y%m%d%H", time.localtime(ts))
        elif strategy == "source_based":
            return f"src_{item.get('source', 'default')}"
        else:  # round_robin
            return f"pt_{index % 8:02d}"

    @staticmethod
    def _convert_format(
        item: dict[str, Any], fmt: str,
    ) -> dict[str, Any]:
        """格式转换：JSON/CSV/Parquet 模拟。"""
        if fmt == "csv":
            return {"format": "csv", "raw": str(item)}
        elif fmt == "parquet":
            return {"format": "parquet", "columns": list(item.keys()), "rows": 1}
        return {"format": "json", "payload": item}

    @staticmethod
    def _simulate_write(partition: str) -> bool:
        """模拟写入成功（绝大部分成功，偶发失败模拟异常）。"""
        return hash(partition) % 20 != 0

    def verify_output_integrity(self, sink_name: str) -> dict[str, Any]:
        """验证指定 sink 下的输出记录完整性和写入成功率。

        只统计每个逻辑分区的最新状态，因此重试不会让总量持续增长。
        """
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == sink_name and r.get("write_status")
        ]
        latest: dict[str, dict[str, Any]] = {}
        for record in records:
            key = str(record.get("_batch_partition") or record.get("partition", ""))
            latest[key] = record
        total = len(latest)
        success = sum(1 for r in latest.values() if r.get("write_status") == "success")
        failed = sum(1 for r in latest.values() if r.get("write_status") == "failed")
        abandoned = sum(1 for r in latest.values() if r.get("write_status") == "abandoned")
        return {
            "ok": True,
            "sink": sink_name,
            "total": total,
            "success": success,
            "failed": failed,
            "abandoned": abandoned,
            "success_rate": success / max(total, 1),
            "partitions": total,
            "needs_retry": failed > 0,
        }

    def retry_failed_writes(self, sink_name: str, batch_id: str | None = None,
                            request_id: str | None = None) -> dict[str, Any]:
        """重试失败的写入：替换同一逻辑分区的失败状态，而不是追加新记录。"""
        if batch_id is None:
            open_batches = batch_ledger.list_batches(sink_name=sink_name, status="collecting")
            if not open_batches:
                return {
                    "ok": True, "sink": sink_name, "total_failed": 0,
                    "retried_success": 0, "batch_id": None,
                    "message": "没有待重试的失败写入",
                }
            batch_id = open_batches[0]["batch_id"]

        result = batch_ledger.attempt(batch_id, request_id=request_id)
        if not result.get("ok"):
            return {"ok": False, "sink": sink_name, "batch_id": batch_id,
                    "error": result.get("error"), "code": result.get("code")}
        return {
            "ok": True,
            "sink": sink_name,
            "batch_id": batch_id,
            "total_failed": len(result["failed"]),
            "retried_success": len(result["success"]),
            "replaced": result["replaced"],
            "attempt": result["attempt"],
            "report": result["report"],
        }

    def list_sink_destinations(self) -> list[dict[str, Any]]:
        """列出所有活跃的输出目标和状态。"""
        configs = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        result = []
        for c in configs:
            name = c.get("配置名称", "")
            records_count = sum(
                1 for r in store.rows(self.MODULE)
                if r.get("sink") == name
            )
            result.append({
                "sink": name,
                "type": c.get("输出类型", ""),
                "target": c.get("目标地址", ""),
                "status": c.get("status", ""),
                "records": records_count,
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_records"] = sum(
            1 for r in rows if r.get("write_status")
        )
        base["failed_records"] = sum(
            1 for r in rows if r.get("write_status") == "failed"
        )
        return base