"""输出批次接口：批次登记、重试、封口、完整性报告与检查点闸门核对。

这些接口不挂在通用资源路由下，原因有二：
  * 批次是跨模块（聚合落库 / 结果落地 / 检查点）的流程对象，不在单一模块的列表口径内；
  * 结论都要带报告版本，避免和 /api/{module}/{id} 的动态路径混淆。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.output_batches import batch_ledger
from app.services import service_for

router = APIRouter(prefix="/api/output-batches", tags=["输出批次"])


class BatchOpenPayload(BaseModel):
    task_name: str
    sink_name: str
    source: str = "sink_output"
    version: str | None = None
    partitions: list[str] | dict[str, Any] = Field(default_factory=list)
    deadline_seconds: float = 300.0
    request_id: str | None = None


class BatchAttemptPayload(BaseModel):
    request_id: str | None = None
    outcomes: dict[str, bool] | None = None
    forced_failures: list[str] | None = None
    extend_seconds: float = 0.0


class BatchSealPayload(BaseModel):
    abandon_failed: bool = False
    request_id: str | None = None
    abandon_reason: str = "到达批次截止时间，放弃失败分区并带缺口发布"


def _require_batch(batch_id: str) -> dict[str, Any]:
    batch = batch_ledger.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"输出批次 {batch_id} 不存在")
    return batch


@router.get("")
def list_batches(
    task_name: str | None = Query(default=None),
    sink_name: str | None = Query(default=None),
    source: str | None = Query(default=None),
    status: str | None = Query(default=None),
    version: str | None = Query(default=None),
) -> dict[str, Any]:
    items = batch_ledger.list_batches(
        task_name=task_name, sink_name=sink_name, source=source,
        status=status, version=version,
    )
    return {"items": items, "total": len(items)}


@router.get("/reports")
def list_reports(
    task_name: str | None = Query(default=None),
    source: str | None = Query(default=None),
) -> dict[str, Any]:
    items = batch_ledger.list_reports(task_name=task_name, source=source)
    return {"items": items, "total": len(items)}


@router.post("")
def open_batch(payload: BatchOpenPayload) -> dict[str, Any]:
    if not payload.partitions:
        raise HTTPException(status_code=400, detail="批次至少需要一个逻辑分区")
    return batch_ledger.open_batch(
        task_name=payload.task_name,
        sink_name=payload.sink_name,
        partitions=payload.partitions,
        version=payload.version or f"batch_{payload.task_name}",
        source=payload.source,
        deadline_seconds=payload.deadline_seconds,
        request_id=payload.request_id,
    )


@router.get("/{batch_id}")
def get_batch(batch_id: str) -> dict[str, Any]:
    confirm = batch_ledger.confirm_batch(batch_id)
    if not confirm.get("ok"):
        raise HTTPException(status_code=404, detail=confirm["error"])
    return confirm


@router.post("/{batch_id}/attempts")
def attempt_batch(batch_id: str, payload: BatchAttemptPayload) -> dict[str, Any]:
    _require_batch(batch_id)
    result = batch_ledger.attempt(
        batch_id,
        request_id=payload.request_id,
        outcomes=payload.outcomes,
        forced_failures=set(payload.forced_failures) if payload.forced_failures else None,
        extend_seconds=payload.extend_seconds,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.post("/{batch_id}/seal")
def seal_batch(batch_id: str, payload: BatchSealPayload) -> dict[str, Any]:
    _require_batch(batch_id)
    result = batch_ledger.seal(
        batch_id,
        abandon_failed=payload.abandon_failed,
        request_id=payload.request_id,
        abandon_reason=payload.abandon_reason,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.get("/{batch_id}/report")
def batch_report(batch_id: str, version: str | None = Query(default=None)) -> dict[str, Any]:
    _require_batch(batch_id)
    return batch_ledger.integrity_report(batch_id, version=version)


@router.get("/{batch_id}/gate")
def batch_gate(batch_id: str, version: str | None = Query(default=None)) -> dict[str, Any]:
    _require_batch(batch_id)
    service = service_for("checkpoint_mgr")
    return service.verify_batch_gate(batch_id, version)
