"""状态存储领域服务：管理流式任务的有状态存储配置。

数据流角色: calc_operator → state_store（消费者+生产者）→ checkpoint_mgr / replay_engine
增强能力: 引用感知的TTL清理、共享状态键引用索引、增量/全量模式切换、状态键去重、
          状态查询、跨模块状态索引、状态恢复数据版本与阈值核对。

序号29的修复点：TTL 清理不再按时间戳把同键的所有状态一起停用，而是先读取状态键、
按算子归并引用，确认所有引用都过期且无检查点占用后才停用；重复清理不推进删除时点，
同一提交只形成一份批量回执。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import state_lifecycle as lifecycle
from app.services.base import BaseModuleService
from app.store import store


class StateStoreService(BaseModuleService):
    MODULE = "state_store"
    ENTITY = "状态配置"
    LIST_FIELDS = ["配置名称", "后端类型", "状态键模式", "TTL时长", "检查点间隔", "增量模式", "清理策略"]
    REQUIRED_FIELDS = ["配置名称", "后端类型", "状态键模式"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已生效"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # ---- 状态键与引用 ----

    def key_index(self) -> dict[str, Any]:
        """状态键 → 引用算子视图（状态页面使用）。"""
        return lifecycle.describe_state_key_index()

    def compact_duplicate_states(self, config_name: str) -> dict[str, Any]:
        """压缩重复状态：按状态键+算子去重，保留最新版本并写压缩清单。

        同一状态键被多个算子引用属于共享引用，不做压缩。
        """
        return lifecycle.compact_duplicate_states(config_name)

    def apply_ttl_cleanup(
        self,
        config_name: str,
        mode: str | None = None,
        *,
        dry_run: bool = False,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """根据TTL策略清理过期状态条目（引用感知 + 幂等）。

        mode=deferred：同键仍有活动引用时整键保留，等最后一个引用释放后统一清理；
        mode=refcount：按引用计数逐次递减，只清理本轮已过期的引用。
        """
        return lifecycle.apply_ttl_cleanup(
            config_name, mode, dry_run=dry_run, request_id=request_id,
        )

    def query_state_by_operator(
        self, operator_name: str, group_key: str | None = None,
    ) -> dict[str, Any]:
        """按算子名称查询关联状态，支持分组过滤。"""
        matches = []
        for r in store.rows(self.MODULE):
            if lifecycle.operator_of(r) != operator_name:
                continue
            if lifecycle.is_inactive_row(r):
                continue
            if group_key and lifecycle.group_of(r) != group_key:
                continue
            matches.append({
                "state_id": r.get("id"),
                "state_key": lifecycle.state_key_of(r),
                "config": r.get("配置名称", ""),
                "operator": lifecycle.operator_of(r),
                "group": lifecycle.group_of(r),
                "aggregates": r.get("_aggregates", {}),
                "timestamp": lifecycle.last_active_of(r) or 0,
                "status": r.get("status", ""),
            })
        return {
            "ok": True,
            "operator": operator_name,
            "group_filter": group_key,
            "count": len(matches),
            "states": matches,
        }

    def list_active_state_keys(self) -> list[dict[str, Any]]:
        """列出所有活跃的状态键及其引用算子。"""
        result = []
        for item in lifecycle.describe_state_key_index()["keys"]:
            if item["active_rows"] and item["operators"]:
                result.append({
                    "state_key": item["state_key"],
                    "pattern": item["state_key"],
                    "operators": item["operators"],
                    "count": item["active_rows"],
                    "checkpoint_ids": item["checkpoint_ids"],
                })
        return sorted(result, key=lambda entry: entry["count"], reverse=True)

    # ---- 状态恢复数据 ----

    def restore_versions(self, state_key: str | None = None) -> list[dict[str, Any]]:
        return lifecycle.list_restore_data(state_key)

    def threshold_check(
        self,
        config_name: str,
        mode: str | None = None,
        *,
        scope: str | None = None,
        min_groups: int = 0,
    ) -> dict[str, Any]:
        """阈值核对：状态恢复数据必须保留全部仍有引用的分组。"""
        return lifecycle.verify_restore_threshold(
            config_name, mode, scope=scope, min_groups=min_groups,
        )

    def cleanup_receipts(self, config_name: str | None = None) -> list[dict[str, Any]]:
        return lifecycle.list_receipts(config_name)

    def todos(self) -> list[dict[str, Any]]:
        return lifecycle.list_todos()

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        index = lifecycle.build_key_index()
        base["active_states"] = sum(
            1 for r in rows
            if r.get("status") == "已生效" and not lifecycle.is_inactive_row(r)
        )
        base["compacted_states"] = sum(1 for r in rows if r.get("_compacted"))
        base["cleaned_states"] = sum(
            1 for r in rows if lifecycle.deleted_at_of(r) is not None
        )
        base["unique_key_patterns"] = len(index)
        base["shared_state_keys"] = sum(
            1 for info in index.values() if len(info["operators"]) > 1
        )
        base["pinned_state_keys"] = sum(
            1 for info in index.values() if info["pinned"]
        )
        base["restore_versions"] = len(store.rows(lifecycle.RESTORE_TABLE))
        base["open_todos"] = len(lifecycle.list_todos(status="待处理"))
        base["updated_at"] = time.time()
        return base
