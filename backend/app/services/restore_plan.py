"""故障恢复领域服务：定义流式任务的故障恢复方案。

数据流角色: checkpoint_mgr / alert_policy → restore_plan（消费者）→ replay_engine（消费者）
增强能力: 恢复方案演练/启用、恢复模式选择、位点策略管理、状态复用验证、恢复后校验
"""
from __future__ import annotations

import time
from typing import Any

from app.services import state_lifecycle as lifecycle
from app.services.base import BaseModuleService
from app.store import store


class RestorePlanService(BaseModuleService):
    MODULE = "restore_plan"
    ENTITY = "恢复方案"
    LIST_FIELDS = ["方案名称", "任务范围", "恢复模式", "源位点策略", "状态复用", "跳过坏数据", "验证步骤"]
    REQUIRED_FIELDS = ["方案名称", "任务范围", "恢复模式"]
    STATUS_ORDER = ["草稿", "已演练", "已生效"]
    ACTION_RULES = {"演练方案": "草稿", "启用方案": "已演练"}
    NEGATIVE_ACTIONS = []

    RESTORE_MODES = {"from_checkpoint", "restart", "escalation_response", "manual"}

    # ---- 跨模块数据管理 ----

    def dry_run_restore(self, plan_name: str) -> dict[str, Any]:
        """演练恢复方案：模拟执行恢复流程，验证各步骤可行性。"""
        plan = None
        for r in store.rows(self.MODULE):
            if r.get("方案名称") == plan_name:
                plan = r
                break
        if plan is None:
            return {"ok": False, "error": f"恢复方案「{plan_name}」不存在"}

        mode = plan.get("恢复模式", "restart")
        scope = plan.get("任务范围", "")
        offset_strategy = plan.get("源位点策略", "latest_snapshot")
        skip_bad = plan.get("跳过坏数据", "false")
        state_reuse = plan.get("状态复用", "")

        steps = []
        # Step 1: 检查位点策略
        if offset_strategy == "latest_snapshot":
            cp_available = sum(
                1 for r in store.rows("checkpoint_mgr")
                if r.get("checkpoint_id") and r.get("status") == "已启用"
            )
            steps.append({
                "step": "checkpoint_check",
                "ok": cp_available > 0,
                "detail": f"可用检查点: {cp_available}",
            })
        else:
            steps.append({
                "step": "offset_strategy",
                "ok": True,
                "detail": f"位点策略: {offset_strategy}",
            })

        # Step 2: 状态复用校验
        if state_reuse:
            has_state = any(
                scope in str(r.get("配置名称", ""))
                for r in store.rows("state_store")
            )
            steps.append({
                "step": "state_reuse_check",
                "ok": has_state,
                "detail": f"状态复用: {state_reuse}, 可用: {has_state}",
            })
        else:
            steps.append({
                "step": "state_reuse",
                "ok": True,
                "detail": "无状态复用需求",
            })

        # Step 3: 恢复模式
        mode_valid = mode in self.RESTORE_MODES
        steps.append({
            "step": "restore_mode",
            "ok": mode_valid,
            "detail": f"恢复模式: {mode}",
        })

        # Step 4: 跳过坏数据处理
        steps.append({
            "step": "skip_bad_data",
            "ok": True,
            "detail": f"跳过坏数据: {skip_bad}",
        })

        # Step 5: 验证步骤解析
        verify_steps = [
            s.strip()
            for s in plan.get("验证步骤", "").split(",")
            if s.strip()
        ]
        steps.append({
            "step": "verification_steps",
            "ok": len(verify_steps) > 0,
            "detail": f"验证步骤: {verify_steps}",
        })

        all_ok = all(s["ok"] for s in steps)
        if all_ok:
            plan["status"] = "已演练"
            plan["pending"] = True

        return {
            "ok": all_ok,
            "plan": plan_name,
            "mode": mode,
            "steps": steps,
            "all_steps_passed": all_ok,
        }

    def select_optimal_recovery_mode(
        self, scope: str, trigger_count: int,
    ) -> dict[str, Any]:
        """根据触发次数和范围选择最优恢复模式。"""
        if trigger_count == 0:
            return {"mode": "none", "reason": "无需恢复", "scope": scope}

        cp_count = sum(
            1 for r in store.rows("checkpoint_mgr")
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        )
        state_count = sum(
            1 for r in store.rows("state_store")
            if scope in str(r.get("配置名称", ""))
        )

        if trigger_count <= 2:
            mode = "restart"
            reason = "触发次数少，直接重启即可"
        elif trigger_count <= 5 and cp_count > 0:
            mode = "from_checkpoint"
            reason = f"检测到 {trigger_count} 次触发，从 {cp_count} 个检查点恢复"
        elif trigger_count > 5:
            mode = "escalation_response"
            reason = f"持续 {trigger_count} 次触发，需要告警升级处理"
        else:
            mode = "manual"
            reason = "条件不满足自动恢复，需人工介入"

        return {
            "mode": mode,
            "reason": reason,
            "scope": scope,
            "trigger_count": trigger_count,
            "available_checkpoints": cp_count,
            "available_states": state_count,
        }

    def verify_restore_completion(self, plan_name: str) -> dict[str, Any]:
        """验证恢复方案执行完成后的状态一致性。"""
        plan = None
        for r in store.rows(self.MODULE):
            if r.get("方案名称") == plan_name:
                plan = r
                break
        if plan is None:
            return {"ok": False, "error": f"恢复方案「{plan_name}」不存在"}

        scope = plan.get("任务范围", "")
        checks = []

        # 检查 replay_engine 中的回放任务
        replay_done = any(
            scope in str(t.get("目标拓扑", ""))
            and t.get("status") == "已完成"
            for t in store.rows("replay_engine")
        )
        checks.append({"source": "replay_engine", "ok": replay_done})

        # 检查 state_store 中的恢复状态
        state_restored = any(
            scope in str(s.get("配置名称", "")) or "replay" in str(s.get("配置名称", ""))
            for s in store.rows("state_store")
        )
        checks.append({"source": "state_store", "ok": state_restored})

        # 检查状态恢复数据：仍有引用的分组必须全部保留，且每个键只有一个活动版本
        restore_check = self.check_restore_data(scope)
        checks.append({
            "source": "state_restore_data",
            "ok": restore_check["all_valid"],
            "detail": restore_check["summary"],
        })

        all_ok = all(c["ok"] for c in checks)
        if all_ok:
            plan["status"] = "已生效"
            plan["pending"] = False

        return {
            "ok": all_ok,
            "plan": plan_name,
            "scope": scope,
            "checks": checks,
        }

    def check_restore_data(self, scope: str) -> dict[str, Any]:
        """按范围核对状态恢复数据：分组齐全、来源版本可追溯、活动版本唯一。"""
        index = lifecycle.build_key_index()
        keys = []
        for key in sorted(index):
            info = index[key]
            # 只核对真正承载算子引用的状态键，跳过纯配置行
            has_operator_ref = any(
                row.get("_operator") or row.get("_state_key") or row.get("state_key")
                for row in info["rows"]
            )
            has_version = lifecycle.active_restore_version(key) is not None
            if not (has_operator_ref or has_version):
                continue
            if scope and scope not in key and not any(
                scope in str(row.get("配置名称", "")) for row in info["rows"]
            ):
                continue
            keys.append(key)
        entries = []
        for key in keys:
            active = lifecycle.active_restore_version(key)
            info = index[key]
            ttl = lifecycle.parse_ttl(
                info["rows"][0].get("TTL时长") if info["rows"] else None, None,
            )
            expected = {
                (item["operator"], item["group"])
                for item in lifecycle.group_payload(info, time.time(), ttl)
            }
            retained = {
                (item["operator"], item["group"])
                for item in (active or {}).get("groups", [])
            }
            needs_version = active is not None or info["pinned"]
            entries.append({
                "state_key": key,
                "restore_id": active.get("restore_id") if active else None,
                "source_version": active.get("source_version") if active else None,
                "pinned": info["pinned"],
                "expected_groups": len(expected),
                "retained_groups": len(retained),
                "missing_groups": [
                    {"operator": operator, "group": group}
                    for operator, group in sorted(expected - retained)
                ],
                "status": (
                    "已生成" if active is not None
                    else ("缺状态恢复数据" if info["pinned"] else "未生成")
                ),
                "valid": (not (expected - retained)) if active is not None
                else not needs_version,
            })
        valid_count = sum(1 for entry in entries if entry["valid"])
        return {
            "ok": True,
            "scope": scope,
            "entries": entries,
            "all_valid": bool(entries) and valid_count == len(entries),
            "summary": f"{valid_count}/{len(entries)} 个状态键的恢复数据完整",
        }

    def list_recovery_plans_with_status(self) -> list[dict[str, Any]]:
        """列出所有恢复方案及其关联的回放和检查点状态。"""
        plans = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 1000 and r.get("方案名称")
        ]
        result = []
        for p in plans:
            name = p.get("方案名称", "")
            scope = p.get("任务范围", "")
            # 关联回放引擎
            linked_replays = [
                t for t in store.rows("replay_engine")
                if scope in str(t.get("目标拓扑", ""))
            ]
            # 关联检查点
            linked_cps = [
                c for c in store.rows("checkpoint_mgr")
                if c.get("checkpoint_id") and c.get("status") == "已启用"
            ]
            result.append({
                "plan": name,
                "mode": p.get("恢复模式", ""),
                "scope": scope,
                "status": p.get("status", ""),
                "linked_replays": len(linked_replays),
                "available_checkpoints": len(linked_cps),
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["drilled"] = sum(1 for r in rows if r.get("status") == "已演练")
        base["activated"] = sum(1 for r in rows if r.get("status") == "已生效")
        return base