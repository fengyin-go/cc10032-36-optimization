"""实时计算服务中心 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.output_batches import seed_demo_batches
from app.routers import ROUTERS
from app.store import store

app = FastAPI(title="实时计算服务中心", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# ---- 数据通道 API ----

@app.get("/api/channels")
def list_channels() -> dict[str, Any]:
    """列出所有数据通道事件及统计。"""
    channels = list(store._channels.values())
    return {
        "total": len(channels),
        "pending": sum(1 for c in channels if c.status == "pending"),
        "delivered": sum(1 for c in channels if c.status == "delivered"),
        "consumed": sum(1 for c in channels if c.status == "consumed"),
        "failed": sum(1 for c in channels if c.status == "failed"),
        "channels": [
            {
                "channel_id": c.channel_id,
                "source": c.source_module,
                "target": c.target_module,
                "type": c.data_type,
                "status": c.status,
                "retries": c.retry_count,
            }
            for c in channels
        ],
    }


@app.get("/api/channels/{target_module}")
def list_module_channels(target_module: str) -> dict[str, Any]:
    """列出目标模块的待处理数据通道。"""
    pending = store.get_pending_channels(target_module)
    return {
        "target": target_module,
        "pending_count": len(pending),
        "channels": [
            {
                "channel_id": c.channel_id,
                "source": c.source_module,
                "type": c.data_type,
                "status": c.status,
            }
            for c in pending
        ],
    }


# ---- 失败恢复 API ----

@app.get("/api/failures")
def list_failures(module: str | None = None) -> dict[str, Any]:
    """列出失败恢复记录。"""
    return store.get_failure_stats()


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)

# 演示批次在应用启动时写入一次，页面刷新后仍能从台账读到封口后的报告和历史记录。
seed_demo_batches()
