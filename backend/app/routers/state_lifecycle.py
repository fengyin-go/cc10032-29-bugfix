"""序号29业务接口：状态键引用视图、引用感知清理、状态恢复数据与检查点校验。

路由层不做业务判断，全部转给领域服务：
  state_store      → 状态键索引、清理回执、恢复数据版本、阈值核对
  checkpoint_mgr  → 压缩清单、按活动引用验证恢复数据
  replay_engine   → 回放发布校验
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services import state_lifecycle as lifecycle
from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.replay_engine import ReplayEngineService
from app.services.restore_plan import RestorePlanService
from app.services.state_store import StateStoreService
from app.store import store

router = APIRouter(prefix="/api/state-lifecycle", tags=["状态生命周期"])

state_service = StateStoreService()
checkpoint_service = CheckpointMgrService()
replay_service = ReplayEngineService()
restore_service = RestorePlanService()


def _require_config(config_name: str) -> None:
    exists = any(
        row.get("配置名称") == config_name
        for row in store.rows(lifecycle.STATE_MODULE)
    )
    if not exists:
        raise HTTPException(status_code=404, detail=f"状态配置「{config_name}」不存在")


@router.get("/keys")
def list_state_keys() -> dict[str, Any]:
    """状态键 → 引用算子视图（状态页面）。"""
    return state_service.key_index()


@router.get("/todos")
def list_todos(
    module: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """共享键等待释放 / 检查点占用 / 发布阻断产生的跨页面待办。"""
    rows = lifecycle.list_todos(module, status)
    return {"items": rows, "total": len(rows)}


@router.post("/todos/{todo_id}/resolve")
def resolve_todo(todo_id: int) -> dict[str, Any]:
    result = lifecycle.resolve_todo(todo_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/state-keys/{config_name}/plan")
def cleanup_plan(
    config_name: str,
    mode: str | None = Query(default=None),
) -> dict[str, Any]:
    """清理预演：只读判断每个键会不会被停用。"""
    _require_config(config_name)
    return lifecycle.evaluate_cleanup_plan(config_name, mode)


@router.post("/state-keys/{config_name}/cleanup")
def run_cleanup(
    config_name: str,
    mode: str | None = Query(default=None),
    request_id: str | None = Query(default=None),
    dry_run: bool = Query(default=False),
) -> dict[str, Any]:
    """引用感知的TTL清理：重复提交只落一份回执。"""
    _require_config(config_name)
    return state_service.apply_ttl_cleanup(
        config_name, mode, dry_run=dry_run, request_id=request_id,
    )


@router.get("/state-keys/{config_name}/receipts")
def cleanup_receipts(config_name: str) -> dict[str, Any]:
    _require_config(config_name)
    items = state_service.cleanup_receipts(config_name)
    return {"items": items, "total": len(items)}


@router.get("/restore-data/plan-check")
def restore_plan_check(scope: str = Query(default="")) -> dict[str, Any]:
    """故障恢复页：按范围核对状态恢复数据的完整性。"""
    return restore_service.check_restore_data(scope)


@router.get("/restore-data")
def restore_data(
    state_key: str | None = Query(default=None),
    mode: str | None = Query(default=None),
    active_only: bool = Query(default=False),
) -> dict[str, Any]:
    """状态恢复数据版本；每个状态键最多一条活动版本。"""
    items = lifecycle.list_restore_data(state_key, mode, active_only)
    active = [item for item in items if item.get("status") == "活动"]
    return {
        "items": items,
        "total": len(items),
        "active_count": len(active),
        "active_version": active[0]["restore_id"] if len(active) == 1 else None,
    }


@router.get("/restore-data/{restore_id}/validate")
def validate_restore_data(
    restore_id: str,
    checkpoint_id: str | None = Query(default=None),
) -> dict[str, Any]:
    result = lifecycle.validate_restore_data(restore_id, checkpoint_id)
    if result.get("error") and not result.get("valid"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/state-keys/{config_name}/threshold")
def threshold_check(
    config_name: str,
    mode: str | None = Query(default=None),
    scope: str | None = Query(default=None),
    min_groups: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """阈值核对页：状态恢复数据是否保留全部仍有引用的分组。"""
    _require_config(config_name)
    return state_service.threshold_check(
        config_name, mode, scope=scope, min_groups=min_groups,
    )


@router.get("/checkpoints/{checkpoint_id}/manifest")
def checkpoint_manifest(checkpoint_id: str) -> dict[str, Any]:
    """检查点详情页：状态压缩清单 + 活动引用 + 规则命中。"""
    manifest = checkpoint_service.checkpoint_manifest(checkpoint_id)
    if not manifest.get("ok"):
        raise HTTPException(status_code=404, detail=manifest.get("error"))
    return manifest


@router.post("/checkpoints/{checkpoint_id}/restore")
def restore_checkpoint(checkpoint_id: str) -> dict[str, Any]:
    return checkpoint_service.restore_state(checkpoint_id)


@router.get("/replay/{task_id}/publish-check")
def replay_publish_check(task_id: int) -> dict[str, Any]:
    """回放页面：确认共享键没有被提前移除，再决定是否发布。"""
    result = replay_service.verify_before_publish(task_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error"))
    return result
