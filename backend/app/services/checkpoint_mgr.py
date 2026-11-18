"""检查点领域服务：管理流式任务的检查点策略。

数据流角色: replay_engine → checkpoint_mgr（消费者+生产者）→ restore_plan（消费者）
增强能力: 检查点创建/快照/恢复/对齐、状态校验、外部化存储路径管理、多检查点一致性验证
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services import state_lifecycle as lifecycle
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
        self,
        config_name: str,
        operator_states: dict[str, Any],
        restore_id: str | None = None,
        state_keys: list[str] | None = None,
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

        manifest = self._build_manifest(state_keys or [], operator_states)
        version = lifecycle.get_restore_version(restore_id) if restore_id else None

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
            "state_key_manifest": manifest["state_keys"],
            "state_compaction_manifest": manifest["entries"],
            "restore_id": restore_id,
            "source_manifest_id": manifest.get("source_manifest_id"),
            "source_version": version.get("restore_id") if version else None,
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
            "state_keys": manifest["state_keys"],
            "restore_id": restore_id,
            "source_version": snapshot["source_version"],
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
            s["_pruned_at"] = time.time()

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
        # 验证状态结构 + 按压缩清单和活动引用核对状态恢复数据
        validation = self._validate_state_structure(states)
        restore_check = self._validate_restore_reference(snapshot)
        validation["restore_data"] = restore_check
        if not restore_check["valid"]:
            validation["valid"] = False
            validation["issues"] = [
                *validation["issues"],
                *[f"状态键缺失: {key}" for key in restore_check["missing_keys"]],
                *[
                    f"分组缺失: {group.get('operator')}/{group.get('group')}"
                    for group in restore_check["missing_groups"]
                ],
            ]

        return {
            "ok": validation["valid"],
            "checkpoint_id": checkpoint_id,
            "config": snapshot.get("配置名称", ""),
            "snapshot_ts": snapshot.get("snapshot_ts", 0),
            "operator_count": len(states),
            "operators": list(states.keys()),
            "states": states,
            "source_version": snapshot.get("source_version")
            or restore_check.get("source_version"),
            "restore_check": restore_check,
            "validation": validation,
        }

    def _validate_restore_reference(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        """按状态压缩清单和活动引用验证状态恢复数据是否还完整。"""
        restore_id = snapshot.get("restore_id")
        if not restore_id:
            return {
                "valid": True,
                "checked": False,
                "reason": "检查点未绑定状态恢复数据，使用状态结构校验",
                "missing_keys": [],
                "missing_groups": [],
                "source_version": snapshot.get("source_version"),
            }
        result = lifecycle.validate_restore_data(
            restore_id, str(snapshot.get("checkpoint_id") or snapshot.get("id")),
        )
        return {
            "valid": bool(result.get("valid")),
            "checked": True,
            "restore_id": restore_id,
            "missing_keys": result.get("missing_keys", []),
            "missing_groups": result.get("missing_groups", []),
            "source_version": result.get("source_version"),
            "source_manifest_id": result.get("source_manifest_id"),
            "rule_hits": result.get("rule_hits", []),
        }

    def checkpoint_manifest(self, checkpoint_id: str) -> dict[str, Any]:
        """检查点详情页：状态压缩清单 + 活动引用 + 规则命中。"""
        manifest = lifecycle.build_checkpoint_manifest(checkpoint_id)
        if not manifest.get("ok"):
            return manifest
        snapshot = next(
            (row for row in store.rows(self.MODULE)
             if str(row.get("checkpoint_id") or row.get("id")) == str(checkpoint_id)),
            None,
        )
        restore_id = (snapshot or {}).get("restore_id")
        if restore_id:
            manifest["restore_validation"] = lifecycle.validate_restore_data(
                restore_id, str(checkpoint_id),
            )
        manifest["source_version"] = (snapshot or {}).get("source_version")
        return manifest

    @staticmethod
    def _build_manifest(
        state_keys: list[str], operator_states: dict[str, Any],
    ) -> dict[str, Any]:
        """把检查点引用的状态键整理成压缩清单。"""
        index = lifecycle.build_key_index()
        latest = lifecycle.latest_manifest_map()
        keys = [str(key) for key in state_keys if key]
        if not keys:
            for operator, state in (operator_states or {}).items():
                if isinstance(state, dict):
                    for field in ("_state_key", "state_key", "状态键"):
                        value = state.get(field)
                        if value:
                            keys.append(str(value))
                    if not keys and operator in index:
                        keys.append(operator)
        keys = sorted(set(keys))
        entries = []
        source_manifest = None
        for key in keys:
            manifest = latest.get(key)
            info = index.get(key, {})
            entries.append({
                "state_key": key,
                "manifest_id": manifest.get("manifest_id") if manifest else None,
                "kept_ids": manifest.get("kept_ids", []) if manifest else [],
                "compacted_ids": manifest.get("compacted_ids", []) if manifest else [],
                "operators": info.get("all_operators", []),
                "groups": info.get("groups", []),
            })
            if manifest is not None and source_manifest is None:
                source_manifest = manifest.get("manifest_id")
        return {"state_keys": keys, "entries": entries, "source_manifest_id": source_manifest}

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
                "restore_id": cp.get("restore_id"),
                "source_version": cp.get("source_version"),
                "state_keys": list(cp.get("state_key_manifest", [])),
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_snapshots"] = sum(1 for r in rows if r.get("checkpoint_id"))
        base["available_snapshots"] = sum(
            1 for r in rows
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        )
        base["pruned_snapshots"] = sum(1 for r in rows if r.get("_pruned"))
        base["pinned_state_keys"] = sum(
            len(cp.get("state_key_manifest", []))
            for cp in rows
            if cp.get("checkpoint_id") and cp.get("status") == "已启用"
            and not cp.get("_pruned")
        )
        return base