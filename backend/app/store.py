"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

增强: DataChannel 跨模块数据通道 + FailureRecovery 失败恢复机制
"""
from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from app.seed import SEED_ROWS


@dataclass
class DataChannelEvent:
    """跨模块数据通道事件：模块间数据传递的统一载体。"""
    channel_id: str
    source_module: str
    target_module: str
    data_type: str
    payload: dict[str, Any]
    timestamp: float
    status: str = "pending"  # pending, delivered, consumed, failed
    retry_count: int = 0
    max_retries: int = 3
    _history: list[dict[str, Any]] = field(default_factory=list)

    def mark_delivered(self) -> None:
        self.status = "delivered"
        self._history.append({"status": "delivered", "ts": time.time()})

    def mark_consumed(self, consumer_response: dict[str, Any] | None = None) -> None:
        self.status = "consumed"
        self._history.append({
            "status": "consumed",
            "ts": time.time(),
            "response": consumer_response,
        })

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self._history.append({"status": "failed", "ts": time.time(), "error": error})

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries


@dataclass
class FailureRecovery:
    """失败恢复记录：跟踪模块间数据流中的失败并执行恢复。"""
    recovery_id: str
    failed_module: str
    failed_operation: str
    error_detail: str
    recovery_strategy: str  # retry, skip, checkpoint, escalate
    context: dict[str, Any]
    status: str = "open"  # open, recovering, recovered, escalated
    created_at: float = field(default_factory=time.time)
    resolved_at: float | None = None

    def attempt_recovery(self) -> dict[str, Any]:
        self.status = "recovering"
        if self.recovery_strategy == "retry":
            return {"action": "retry", "context": self.context, "recovery_id": self.recovery_id}
        elif self.recovery_strategy == "skip":
            self.status = "recovered"
            self.resolved_at = time.time()
            return {"action": "skip", "recovery_id": self.recovery_id}
        elif self.recovery_strategy == "checkpoint":
            return {"action": "restore_from_checkpoint", "recovery_id": self.recovery_id}
        else:  # escalate
            self.status = "escalated"
            return {"action": "escalate", "recovery_id": self.recovery_id}

    def mark_resolved(self) -> None:
        self.status = "recovered"
        self.resolved_at = time.time()


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据通道：channel_id → DataChannelEvent
        self._channels: dict[str, DataChannelEvent] = {}
        # 失败恢复记录：recovery_id → FailureRecovery
        self._failures: dict[str, FailureRecovery] = {}

    # ---- 跨模块数据通道 ----

    def send_to_channel(
        self,
        source: str,
        target: str,
        data_type: str,
        payload: dict[str, Any],
    ) -> DataChannelEvent:
        """通过数据通道将数据从一个模块发送到另一个模块。

        创建 DataChannelEvent 记录并返回，下游模块通过 consume_from_channel 消费。
        """
        import hashlib
        channel_id = hashlib.md5(
            f"{source}_{target}_{data_type}_{time.time()}".encode()
        ).hexdigest()[:16]
        event = DataChannelEvent(
            channel_id=channel_id,
            source_module=source,
            target_module=target,
            data_type=data_type,
            payload=payload,
            timestamp=time.time(),
        )
        self._channels[channel_id] = event
        # 同时写入下游模块的 store 表
        notification = {
            "id": max(
                (int(r.get("id", 0)) for r in self.rows(target)), default=0
            ) + 1,
            "channel_id": channel_id,
            "source": source,
            "data_type": data_type,
            "payload_snapshot": copy.deepcopy(payload),
            "timestamp": event.timestamp,
            "status": "pending",
            "pending": True,
            "abnormal": False,
        }
        self.rows(target).append(notification)
        return event

    def consume_from_channel(
        self, channel_id: str, consumer_module: str,
    ) -> DataChannelEvent | None:
        """消费数据通道中的事件。"""
        event = self._channels.get(channel_id)
        if event is None:
            return None
        if event.target_module != consumer_module:
            return None
        event.mark_consumed()
        # 更新下游模块表中的通知状态
        for row in self.rows(consumer_module):
            if row.get("channel_id") == channel_id:
                row["status"] = "consumed"
                row["pending"] = False
                break
        return event

    def get_pending_channels(self, target_module: str) -> list[DataChannelEvent]:
        """获取目标模块中待消费的数据通道事件。"""
        return [
            e for e in self._channels.values()
            if e.target_module == target_module and e.status == "pending"
        ]

    # ---- 失败恢复 ----

    def register_failure(
        self,
        module: str,
        operation: str,
        error: str,
        strategy: str = "retry",
        context: dict[str, Any] | None = None,
    ) -> FailureRecovery:
        """注册失败事件并创建恢复记录。"""
        import hashlib
        rid = hashlib.md5(
            f"{module}_{operation}_{time.time()}".encode()
        ).hexdigest()[:12]
        recovery = FailureRecovery(
            recovery_id=rid,
            failed_module=module,
            failed_operation=operation,
            error_detail=error,
            recovery_strategy=strategy,
            context=context or {},
        )
        self._failures[rid] = recovery
        return recovery

    def get_open_failures(self, module: str | None = None) -> list[FailureRecovery]:
        """获取未恢复的失败记录。"""
        failures = [
            f for f in self._failures.values()
            if f.status in ("open", "recovering")
        ]
        if module:
            failures = [f for f in failures if f.failed_module == module]
        return failures

    def resolve_failure(self, recovery_id: str) -> FailureRecovery | None:
        """标记失败为已恢复。"""
        recovery = self._failures.get(recovery_id)
        if recovery:
            recovery.mark_resolved()
        return recovery

    def get_failure_stats(self) -> dict[str, Any]:
        """获取失败恢复统计。"""
        all_failures = list(self._failures.values())
        open_count = sum(1 for f in all_failures if f.status in ("open", "recovering"))
        resolved_count = sum(1 for f in all_failures if f.status == "recovered")
        escalated_count = sum(1 for f in all_failures if f.status == "escalated")
        return {
            "total": len(all_failures),
            "open": open_count,
            "resolved": resolved_count,
            "escalated": escalated_count,
        }

    # ---- 基础 CRUD ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        # 附加数据通道和失败恢复统计
        channel_stats = {
            "active_channels": len(self._channels),
            "pending_channels": sum(
                1 for e in self._channels.values() if e.status == "pending"
            ),
        }
        failure_stats = self.get_failure_stats()
        return {
            "cards": cards,
            "modules": modules,
            "channel_stats": channel_stats,
            "failure_stats": failure_stats,
        }


store = Store()