"""回放引擎领域服务：消费检查点数据执行数据回放与恢复验证。

数据流: checkpoint_mgr（检查点管理）→ replay_engine（回放引擎）→ restore_plan（故障恢复）
核心逻辑: 检查点快照回放、状态恢复验证、位点对齐、回放进度管理。
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services import state_lifecycle as lifecycle
from app.services.base import BaseModuleService
from app.store import store


class ReplayEngineService(BaseModuleService):
    MODULE = "replay_engine"
    ENTITY = "回放任务"
    LIST_FIELDS = ["任务名称", "源检查点", "目标拓扑", "回放模式", "起始位点", "结果验证", "进度"]
    REQUIRED_FIELDS = ["任务名称", "源检查点", "回放模式"]
    STATUS_ORDER = ["草稿", "待回放", "回放中", "已完成", "验证失败"]
    ACTION_RULES = {"启动回放": "待回放", "暂停回放": "回放中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = ["暂停回放"]

    def create_replay_job(
        self,
        job_name: str,
        checkpoint_id: str,
        topology_name: str,
        mode: str = "full_replay",
    ) -> dict[str, Any]:
        """基于检查点创建数据回放任务。

        消费 checkpoint_mgr 中的检查点快照，结合 task_topology 的拓扑配置，
        创建回放任务并写入恢复方案的引用。
        """
        # 1. 验证检查点存在
        cp_row = None
        for r in store.rows("checkpoint_mgr"):
            cid = r.get("id", "")
            if str(cid) == str(checkpoint_id):
                cp_row = r
                break
        if cp_row is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}

        if cp_row.get("status") != "已启用":
            return {"ok": False, "error": f"检查点「{checkpoint_id}」未启用"}

        # 2. 验证拓扑配置
        topo_found = any(
            r.get("拓扑名称") == topology_name
            for r in store.rows("task_topology")
        )
        if not topo_found and topology_name:
            return {"ok": False, "error": f"拓扑配置「{topology_name}」不存在"}

        restore_id = cp_row.get("restore_id")
        version = lifecycle.get_restore_version(restore_id) if restore_id else None
        restore_check = None
        if restore_id:
            restore_check = lifecycle.validate_restore_data(
                restore_id, str(checkpoint_id),
            )
            if not restore_check.get("valid"):
                return {
                    "ok": False,
                    "error": "状态恢复数据缺键，阻止创建回放任务",
                    "restore_check": restore_check,
                }

        # 3. 创建回放任务
        task_id = max(
            (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
        ) + 1
        replay_task = {
            "id": task_id,
            "任务名称": job_name,
            "源检查点": str(checkpoint_id),
            "目标拓扑": topology_name,
            "回放模式": mode,
            "起始位点": cp_row.get("timestamp", time.time() - 3600),
            "结果验证": "待验证",
            "进度": "0%",
            "status": "待回放",
            "pending": True,
            "abnormal": False,
            "_checkpoint_snapshot": copy.deepcopy(cp_row),
            "restore_id": restore_id,
            "source_version": (
                version.get("restore_id") if version
                else cp_row.get("source_version")
            ),
            "_restore_check": copy.deepcopy(restore_check),
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(replay_task)

        return {
            "ok": True,
            "task_id": task_id,
            "task_name": job_name,
            "checkpoint_id": checkpoint_id,
            "topology": topology_name,
            "mode": mode,
            "restore_id": restore_id,
            "source_version": replay_task["source_version"],
        }

    def execute_replay(self, task_id: int) -> dict[str, Any]:
        """执行回放任务：检查点状态快照回放 + 结果验证。

        发布当前回放结果前，按状态压缩清单和活动引用重新校验状态恢复数据；
        缺键时阻止发布，任务停留在「验证失败」并写入待办。
        """
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}

        if task.get("status") not in ("待回放", "回放中"):
            return {"ok": False, "error": f"任务状态为「{task.get('status')}」, 不可回放"}

        task["status"] = "回放中"

        restore_id = task.get("restore_id")
        if restore_id:
            restore_check = lifecycle.validate_restore_data(
                restore_id, str(task.get("源检查点", "")),
            )
            if not restore_check.get("valid"):
                task["status"] = "验证失败"
                task["结果验证"] = "blocked"
                task["_restore_check"] = restore_check
                task["_publish_blocked_at"] = time.time()
                self._raise_publish_todo(task, restore_check)
                return {
                    "ok": False,
                    "task_id": task_id,
                    "error": "状态恢复数据缺键，阻止发布当前回放结果",
                    "published": False,
                    "restore_check": restore_check,
                }

        # 获取检查点快照中的状态数据
        cp_snapshot = task.get("_checkpoint_snapshot", {})
        state_data = cp_snapshot.get("state", cp_snapshot.get("operator_states", {}))

        # 执行回放：对每个算子状态复制到 state_store
        replay_results = []
        if state_data:
            for op_name, op_state in state_data.items():
                state_entry = {
                    "id": max(
                        (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
                    ) + 1,
                    "配置名称": f"replay_{op_name}_{task_id}",
                    "后端类型": "memory",
                    "状态键模式": f"replay://{op_name}",
                    "TTL时长": "3600",
                    "检查点间隔": "30",
                    "增量模式": "full",
                    "清理策略": "after_replay",
                    "status": "已生效",
                    "pending": True,
                    "abnormal": False,
                    "_replay_task_id": task_id,
                    "_replay_state": copy.deepcopy(op_state),
                    "_created_at": time.time(),
                }
                store.rows("state_store").append(state_entry)
                replay_results.append({
                    "operator": op_name,
                    "state_keys": len(op_state) if isinstance(op_state, dict) else 0,
                    "restored_to": "state_store",
                })

        # 验证回放结果
        verification = self._verify_replay_results(task_id, replay_results)

        task["结果验证"] = verification["status"]
        task["进度"] = "100%"
        task["status"] = "已完成" if verification["all_passed"] else "验证失败"
        task["_replay_results"] = replay_results
        task["published"] = verification["all_passed"]
        task["source_version"] = task.get("source_version")

        return {
            "ok": True,
            "task_id": task_id,
            "restored_operators": len(replay_results),
            "replay_results": replay_results,
            "verification": verification,
        }

    def _raise_publish_todo(
        self, task: dict[str, Any], restore_check: dict[str, Any],
    ) -> None:
        """缺键阻断发布时给回放页面生成待办，重复阻断只更新同一条。"""
        todos = store.rows(lifecycle.TODO_TABLE)
        token = f"publish_blocked:replay_engine:{task.get('id')}"
        for row in todos:
            if row.get("token") == token:
                row["updated_at"] = time.time()
                row["status"] = "待处理"
                return
        todos.append({
            "id": max((int(r.get("id", 0)) for r in todos), default=0) + 1,
            "token": token,
            "kind": "publish_blocked",
            "module": "replay_engine",
            "page": "replay_engine",
            "config": task.get("源检查点", ""),
            "mode": None,
            "state_key": (restore_check.get("missing_keys") or [None])[0],
            "message": "状态恢复数据缺键，当前回放结果未发布",
            "status": "待处理",
            "receipt_id": None,
            "created_at": time.time(),
            "updated_at": time.time(),
        })

    def verify_before_publish(self, task_id: int) -> dict[str, Any]:
        """回放页面确认：共享键没有被提前移除，恢复数据仍保留全部分组。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}
        restore_id = task.get("restore_id")
        if not restore_id:
            return {"ok": True, "publishable": True, "restore_id": None}
        result = lifecycle.validate_restore_data(
            restore_id, str(task.get("源检查点", "")),
        )
        return {
            "ok": True,
            "task_id": task_id,
            "restore_id": restore_id,
            "source_version": task.get("source_version"),
            "publishable": bool(result.get("valid")),
            "restore_check": result,
        }

    def _verify_replay_results(
        self, task_id: int, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """验证回放结果的完整性：检查 state_store 中是否正确落入了回放状态。"""
        restored_ops = set(r["operator"] for r in results)
        found_in_store = set()
        for row in store.rows("state_store"):
            if row.get("_replay_task_id") == task_id:
                cfg_name = row.get("配置名称", "")
                parts = cfg_name.split("_")
                if len(parts) >= 3:
                    found_in_store.add(parts[1])

        missing = restored_ops - found_in_store
        all_passed = len(missing) == 0
        return {
            "status": "passed" if all_passed else "partial",
            "all_passed": all_passed,
            "expected": len(restored_ops),
            "found": len(found_in_store),
            "missing_operators": list(missing),
        }

    def list_replay_history(self) -> list[dict[str, Any]]:
        """列出所有回放任务历史记录及恢复方案关联。"""
        history = []
        for task in store.rows(self.MODULE):
            entry = {
                "task_id": task.get("id"),
                "任务名称": task.get("任务名称", ""),
                "源检查点": task.get("源检查点", ""),
                "回放模式": task.get("回放模式", ""),
                "结果验证": task.get("结果验证", "待验证"),
                "进度": task.get("进度", "0%"),
                "status": task.get("status", ""),
                "restore_id": task.get("restore_id"),
                "source_version": task.get("source_version"),
            }
            # 查找关联的恢复方案
            for rp in store.rows("restore_plan"):
                if task.get("目标拓扑", "") in rp.get("任务范围", ""):
                    entry["linked_restore_plan"] = rp.get("方案名称", "")
                    break
            history.append(entry)
        return history

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，附加回放成功率。"""
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        completed = sum(1 for t in rows if t.get("status") == "已完成")
        failed = sum(1 for t in rows if t.get("status") == "验证失败")
        total_done = completed + failed
        base["replay_success_rate"] = completed / max(total_done, 1)
        base["completed_replays"] = completed
        base["failed_replays"] = failed
        return base