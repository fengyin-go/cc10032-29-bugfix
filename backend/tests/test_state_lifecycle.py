"""序号29回归测试：引用感知的TTL清理、共享状态键与状态恢复数据。

覆盖：
  - 共享状态键被最早配置先清理后，另一个算子仍按活动状态读取；
  - 只有全部算子引用都过期才停用状态键；
  - 检查点引用的键必须保留，缺键时阻止发布回放结果；
  - 重复清理不推进删除时点，同一提交只落一份批量回执；
  - deferred / refcount 两种口径形成不同版本，可消费版本只有一个；
  - 阈值核对保留全部仍有引用的分组，事务失败退回同一口径。
"""
from __future__ import annotations

import copy
import time

import pytest

from app.services import state_lifecycle as lifecycle
from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.replay_engine import ReplayEngineService
from app.services.state_store import StateStoreService
from app.store import store


@pytest.fixture(autouse=True)
def clean_runtime_tables():
    """每个用例前把相关表恢复到种子数据，避免跨用例串味。"""
    from app.seed import SEED_ROWS

    for table in (
        "state_store", "checkpoint_mgr", "replay_engine",
        "task_topology", "restore_plan",
        lifecycle.RESTORE_TABLE,
        lifecycle.RECEIPT_TABLE,
        lifecycle.THRESHOLD_TABLE,
        lifecycle.TODO_TABLE,
        lifecycle.MANIFEST_TABLE,
    ):
        store._tables[table] = [copy.deepcopy(row) for row in SEED_ROWS.get(table, [])]
    yield


def _state_row(
    row_id: int,
    key: str,
    operator: str,
    group: str,
    last_active: float,
    ttl: str | float | None = "60",
    config: str | None = None,
    **extra,
):
    row = {
        "id": row_id,
        "配置名称": config or f"cfg_{key}",
        "后端类型": "memory",
        "状态键模式": key,
        "TTL时长": str(ttl) if ttl is not None else "60",
        "检查点间隔": "30",
        "增量模式": "full",
        "清理策略": "ttl_expire",
        "status": "已生效",
        "pending": True,
        "abnormal": False,
        "_state_key": key,
        "_operator": operator,
        "_group": group,
        "_aggregates": {"count": 1.0},
        "_timestamp": last_active,
    }
    row.update(extra)
    if ttl is not None:
        row["_ttl_seconds"] = lifecycle.parse_ttl(ttl, 60.0)
    return row


def _seed_shared_key(config: str, key: str, now: float, active_operator: str, expired_operator: str):
    rows = store.rows("state_store")
    rows.extend([
        _state_row(9001, key, expired_operator, "g_expired", now - 1000, config=config),
        _state_row(9002, key, active_operator, "g_active", now - 5, config=config),
    ])
    return rows


# ---------------------------------------------------------------------------
# 共享键与引用判定
# ---------------------------------------------------------------------------

class TestSharedStateKeyCleanup:
    def test_shared_key_kept_while_other_operator_active(self):
        """最早配置先清理时，共享键的另一算子仍活动，状态不能被停用。"""
        now = time.time()
        _seed_shared_key("cfg_shared", "operator://window/sum", now, "opB", "opA")
        svc = StateStoreService()

        result = svc.apply_ttl_cleanup("cfg_shared", lifecycle.MODE_DEFERRED)

        assert result["ok"] is True
        assert result["cleaned"] == 0
        assert result["retained_keys"] == ["operator://window/sum"]
        rows = {row["id"]: row for row in store.rows("state_store")}
        assert rows[9002]["status"] == "已生效"
        assert rows[9002].get("_deleted_at") is None
        assert rows[9001]["status"] == "已生效", "被引用共享键不能提前停用"

        states = svc.query_state_by_operator("opB")
        assert states["count"] == 1
        assert states["states"][0]["aggregates"] == {"count": 1.0}

    def test_key_cleaned_only_when_all_references_expire(self):
        """全部算子引用都过期后才允许停用状态键。"""
        now = time.time()
        _seed_shared_key("cfg_all", "operator://window/sum", now, "opB", "opA")
        for row in store.rows("state_store"):
            if row.get("id") == 9002:
                row["_timestamp"] = now - 900

        result = lifecycle.apply_ttl_cleanup("cfg_all", "deferred", now)
        assert result["cleaned"] == 2
        assert result["cleaned_keys"] == ["operator://window/sum"]
        for row in store.rows("state_store"):
            if row.get("id") in (9001, 9002):
                assert row["status"] == "已停用"
                assert row["_deleted_at"] is not None

    def test_earliest_config_cannot_disable_shared_key(self):
        """共享键被最早配置先清理后，另一配置的算子仍按活动状态读取。"""
        now = time.time()
        store.rows("state_store").extend([
            _state_row(9005, "operator://shared/x", "opA", "g1", now - 900,
                       config="cfg_earliest"),
            _state_row(9006, "operator://shared/x", "opB", "g2", now - 5,
                       config="cfg_later"),
        ])
        # 配置 cfg_earliest 的键模式只覆盖自身，但同键的引用必须一起判断
        earliest = next(
            row for row in store.rows("state_store")
            if row.get("配置名称") == "cfg_earliest"
        )
        earliest["状态键模式"] = "operator://shared/x"
        later = next(
            row for row in store.rows("state_store")
            if row.get("配置名称") == "cfg_later"
        )
        later["状态键模式"] = "operator://shared/x"

        plan = lifecycle.evaluate_cleanup_plan("cfg_earliest", "deferred", now)
        key_plan = next(item for item in plan["keys"] if item["state_key"] == "operator://shared/x")
        assert key_plan["action"] == "wait_last_release"

        result = lifecycle.apply_ttl_cleanup("cfg_earliest", "deferred", now)
        assert result["cleaned"] == 0
        rows = {row["id"]: row for row in store.rows("state_store")}
        assert rows[9005]["status"] == "已生效"
        assert rows[9006]["status"] == "已生效"
        assert StateStoreService().query_state_by_operator("opB")["count"] == 1

    def test_different_ttl_per_operator_reference(self):
        """不同算子对同一键的过期时间不同，按引用自身 TTL 判定。"""
        now = time.time()
        rows = store.rows("state_store")
        rows.extend([
            _state_row(9011, "operator://join/x", "opA", "g1", now - 500, ttl="60", config="cfg_ttl"),
            _state_row(9012, "operator://join/x", "opB", "g2", now - 90, ttl="120", config="cfg_ttl"),
        ])
        plan = lifecycle.evaluate_cleanup_plan("cfg_ttl", "deferred", now)
        key_plan = next(item for item in plan["keys"] if item["state_key"] == "operator://join/x")
        assert key_plan["expired_operators"] == ["opA"]
        assert key_plan["live_operators"] == ["opB"]
        assert key_plan["action"] == "wait_last_release"

    def test_state_key_index_reads_key_and_references(self):
        """状态页面能按状态键读取引用算子。"""
        now = time.time()
        _seed_shared_key("cfg_idx", "operator://window/sum", now, "opB", "opA")
        index = StateStoreService().key_index()
        entry = next(item for item in index["keys"] if item["state_key"] == "operator://window/sum")
        assert entry["operators"] == ["opA", "opB"]
        assert entry["all_operators"] == ["opA", "opB"]
        assert len(entry["references"]) == 2


# ---------------------------------------------------------------------------
# 幂等与删除时点
# ---------------------------------------------------------------------------

class TestCleanupIdempotency:
    def test_repeat_cleanup_does_not_advance_deletion_time(self):
        """重复清理不能再次推进同一键的删除时点。"""
        now = time.time()
        store.rows("state_store").append(
            _state_row(9021, "operator://solo", "opA", "g1", now - 500, config="cfg_idem"),
        )
        first = lifecycle.apply_ttl_cleanup("cfg_idem", "deferred", now)
        assert first["cleaned"] == 1
        deleted_at = next(
            row["_deleted_at"] for row in store.rows("state_store")
            if row.get("id") == 9021
        )

        second = lifecycle.apply_ttl_cleanup("cfg_idem", "deferred", now + 100)
        assert second["duplicate"] is True
        assert second["cleaned"] == 0
        assert next(
            row["_deleted_at"] for row in store.rows("state_store")
            if row.get("id") == 9021
        ) == deleted_at

    def test_same_submission_records_once(self):
        """同一请求号并发提交只落一份回执。"""
        now = time.time()
        store.rows("state_store").append(
            _state_row(9031, "operator://once", "opA", "g1", now - 500, config="cfg_once"),
        )
        first = lifecycle.apply_ttl_cleanup(
            "cfg_once", "refcount", now, request_id="req-29",
        )
        second = lifecycle.apply_ttl_cleanup(
            "cfg_once", "refcount", now, request_id="req-29",
        )
        assert first["duplicate"] is False
        assert second["duplicate"] is True
        assert first["receipt_id"] == second["receipt_id"]
        receipts = [
            row for row in store.rows(lifecycle.RECEIPT_TABLE)
            if row.get("config") == "cfg_once"
        ]
        assert len(receipts) == 1

    def test_transaction_failure_rolls_back_same_baseline(self):
        """事务中途失败退回同一口径，不留下半批清理。"""
        now = time.time()
        store.rows("state_store").append(
            _state_row(9041, "operator://tx", "opA", "g1", now - 500, config="cfg_tx"),
        )
        before = copy.deepcopy([
            row for row in store.rows("state_store")
            if row.get("id") == 9041
        ])

        original = lifecycle._write_restore_versions

        def boom(*args, **kwargs):
            raise RuntimeError("版本写入失败")

        lifecycle._write_restore_versions = boom
        try:
            result = lifecycle.apply_ttl_cleanup("cfg_tx", "deferred", now)
        finally:
            lifecycle._write_restore_versions = original

        assert result["ok"] is False
        assert result["rolled_back"] is True
        assert store.rows(lifecycle.RECEIPT_TABLE) == []
        after = [
            row for row in store.rows("state_store")
            if row.get("id") == 9041
        ]
        assert after[0]["status"] == before[0]["status"]
        assert after[0].get("_deleted_at") is None


# ---------------------------------------------------------------------------
# 检查点引用、恢复数据版本与发布
# ---------------------------------------------------------------------------

class TestCheckpointAndRestoreData:
    def _make_checkpoint(self, key: str, operators: int = 1) -> dict:
        cp_svc = CheckpointMgrService()
        cp = cp_svc.create_checkpoint(
            "检查点样例2", {"opA": {"_state_key": key}}, state_keys=[key],
        )
        assert cp["ok"] is True
        return cp

    def test_pinned_key_survives_cleanup(self):
        """清理结果必须保留仍被检查点引用的键。"""
        now = time.time()
        store.rows("state_store").extend([
            _state_row(9051, "operator://pin", "opA", "g1", now - 5000, config="cfg_pin"),
            _state_row(9052, "operator://pin", "opB", "g2", now - 5000, config="cfg_pin"),
        ])
        cp = self._make_checkpoint("operator://pin")

        result = lifecycle.apply_ttl_cleanup("cfg_pin", "deferred", now)
        assert result["cleaned"] == 0
        assert result["held_keys"] == ["operator://pin"]
        rows = {row["id"]: row for row in store.rows("state_store")}
        assert rows[9051]["status"] == "已生效"
        assert rows[9052]["status"] == "已生效"

        restored = CheckpointMgrService().restore_state(cp["checkpoint_id"])
        assert restored["ok"] is True
        assert restored["restore_check"]["valid"] is True

    def test_missing_key_blocks_replay_publish(self):
        """状态恢复数据缺键时阻止发布当前回放结果。"""
        now = time.time()
        store.rows("state_store").extend([
            _state_row(9061, "operator://live", "opA", "g1", now - 5, config="cfg_replay"),
        ])
        store.rows("task_topology").append({
            "id": 990, "拓扑名称": "topo_replay", "status": "运行中",
        })
        cp_svc = CheckpointMgrService()
        cp = cp_svc.create_checkpoint(
            "检查点样例2", {"opA": {"_state_key": "operator://live"}},
            restore_id="restore://operator://live/deferred/v1",
            state_keys=["operator://live"],
        )
        # 构造一个与当前活动引用不一致的历史版本（缺 g1 分组）
        store.rows(lifecycle.RESTORE_TABLE).append({
            "id": 1,
            "restore_id": "restore://operator://live/deferred/v1",
            "state_key": "operator://live",
            "config": "cfg_replay",
            "mode": "deferred",
            "version": 1,
            "status": "活动",
            "source_version": "restore://operator://live/deferred/v0",
            "source_manifest_id": None,
            "groups": [],
            "group_count": 0,
            "operators": [],
            "checkpoint_ids": [cp["checkpoint_id"]],
            "retained": True,
            "receipt_id": None,
            "created_at": now,
        })

        replay = ReplayEngineService()
        created = replay.create_replay_job(
            "回放_缺键", cp["checkpoint_id"], "topo_replay",
        )
        assert created["ok"] is False
        assert "缺键" in created["error"]

        # 绕过创建期校验直接落一个回放任务，确认执行期也会阻断
        store.rows("replay_engine").append({
            "id": 999,
            "任务名称": "回放_缺键_直接",
            "源检查点": cp["checkpoint_id"],
            "目标拓扑": "topo_replay",
            "回放模式": "full_replay",
            "status": "待回放",
            "restore_id": "restore://operator://live/deferred/v1",
            "source_version": "restore://operator://live/deferred/v0",
        })
        executed = replay.execute_replay(999)
        assert executed["ok"] is False
        assert executed["published"] is False
        task = store.find("replay_engine", 999)
        assert task["status"] == "验证失败"
        assert task["结果验证"] == "blocked"

    def test_publish_blocked_when_retained_key_removed(self):
        """活动引用消失后，仍带分组的历史版本不能发布。"""
        now = time.time()
        store.rows("state_store").append(
            _state_row(9111, "operator://gone", "opA", "g1", now - 5, config="cfg_gone"),
        )
        store.rows(lifecycle.RESTORE_TABLE).append({
            "id": 51,
            "restore_id": "restore://operator://gone/deferred/v1",
            "state_key": "operator://gone",
            "config": "cfg_gone",
            "mode": "deferred",
            "version": 1,
            "status": "活动",
            "source_version": None,
            "source_manifest_id": None,
            "groups": [{"operator": "opA", "group": "g1"}],
            "group_count": 1,
            "operators": ["opA"],
            "checkpoint_ids": [],
            "retained": True,
            "receipt_id": None,
            "created_at": now,
        })
        result = lifecycle.apply_ttl_cleanup("cfg_gone", "refcount", now + 100)
        assert result["cleaned"] == 1
        check = lifecycle.validate_restore_data("restore://operator://gone/deferred/v1")
        assert check["valid"] is False
        assert check["missing_keys"] == ["operator://gone"]
        assert any(hit["rule"] == "state_key_still_referenced" for hit in check["rule_hits"])

    def test_source_version_preserved(self):
        """状态恢复数据保留来源版本。"""
        now = time.time()
        store.rows("state_store").append(
            _state_row(9071, "operator://ver", "opA", "g1", now - 600, config="cfg_ver"),
        )
        first = lifecycle.apply_ttl_cleanup("cfg_ver", "refcount", now)
        assert first["version_ids"]
        # 释放后 deferred 统一清理，生成第二个版本
        store.rows("state_store").append(
            _state_row(9072, "operator://ver", "opB", "g1", now - 5, config="cfg_ver"),
        )
        lifecycle.apply_ttl_cleanup("cfg_ver", "deferred", now)
        for row in store.rows("state_store"):
            if row.get("id") in (9071, 9072):
                row["_timestamp"] = now - 10000
        lifecycle.apply_ttl_cleanup("cfg_ver", "deferred", now)

        versions = lifecycle.list_restore_data("operator://ver")
        active = [v for v in versions if v["status"] == "活动"]
        assert len(active) == 1
        assert active[0]["source_version"] is not None
        assert active[0]["source_version"].startswith("restore://operator://ver/")

    def test_released_sharing_checkpoint_follows_active_version(self):
        """检查点跟随活动版本，缺键时恢复数据仍能通过校验。"""
        now = time.time()
        store.rows("state_store").append(
            _state_row(9085, "operator://bound", "opA", "g1", now - 900,
                       config="cfg_bound"),
        )
        cp = self._make_checkpoint("operator://bound")
        first = lifecycle.apply_ttl_cleanup("cfg_bound", "deferred", now)
        assert first["version_ids"]
        snapshot = next(
            row for row in store.rows("checkpoint_mgr")
            if row.get("checkpoint_id") == cp["checkpoint_id"]
        )
        active_id = snapshot["restore_id"]
        assert lifecycle.get_restore_version(active_id)["status"] == "活动"

        # 再次清理不产生新版本，检查点绑定不变
        lifecycle.apply_ttl_cleanup("cfg_bound", "deferred", now + 30)
        assert snapshot["restore_id"] == active_id
        assert snapshot["source_version"] in (None, active_id)

    def test_two_modes_form_different_versions_single_active(self):
        """两种口径形成不同版本，可消费结果只有一个活动版本。"""
        now = time.time()
        store.rows("state_store").extend([
            _state_row(9081, "operator://dual", "opA", "g1", now - 800, config="cfg_dual"),
            _state_row(9082, "operator://dual", "opB", "g2", now - 800, config="cfg_dual"),
            _state_row(9083, "operator://dual", "opC", "g3", now - 5, config="cfg_dual"),
        ])
        lifecycle.apply_ttl_cleanup("cfg_dual", "refcount", now)
        for row in store.rows("state_store"):
            if row.get("id") == 9083:
                row["_timestamp"] = now - 10000
        lifecycle.apply_ttl_cleanup("cfg_dual", "deferred", now)

        versions = lifecycle.list_restore_data("operator://dual")
        modes = {version["mode"] for version in versions}
        assert modes == {"refcount", "deferred"}
        active = [version for version in versions if version["status"] == "活动"]
        assert len(active) == 1
        assert active[0]["mode"] == "deferred"
        assert active[0]["source_version"] in {
            version["restore_id"] for version in versions
        }

    def test_threshold_check_retains_referenced_groups(self):
        """阈值核对页确认状态恢复数据保留全部仍有引用的分组。"""
        now = time.time()
        store.rows("state_store").extend([
            _state_row(9091, "operator://thr", "opA", "g1", now - 800, config="cfg_thr"),
            _state_row(9092, "operator://thr", "opB", "g2", now - 800, config="cfg_thr"),
            _state_row(9093, "operator://thr", "opC", "g3", now - 5, config="cfg_thr"),
        ])
        lifecycle.apply_ttl_cleanup("cfg_thr", "refcount", now)
        result = StateStoreService().threshold_check("cfg_thr", "refcount")
        assert result["ok"] is True
        assert result["passed"] is True
        assert result["receipt"]["expected_groups"] == 1
        assert result["receipt"]["retained_groups"] == 1
        assert result["keys"][0]["missing_groups"] == []

    def test_checkpoint_manifest_view(self):
        """检查点详情页能看到压缩清单 + 活动引用 + 规则命中。"""
        now = time.time()
        store.rows("state_store").extend([
            _state_row(9101, "operator://manifest", "opA", "g1", now - 5, config="cfg_man"),
            _state_row(9102, "operator://manifest", "opA", "g1", now - 50, config="cfg_man"),
        ])
        compacted = StateStoreService().compact_duplicate_states("cfg_man")
        assert compacted["compacted"] == 1
        cp = self._make_checkpoint("operator://manifest")
        manifest = CheckpointMgrService().checkpoint_manifest(cp["checkpoint_id"])
        assert manifest["ok"] is True
        entry = next(
            item for item in manifest["entries"]
            if item["state_key"] == "operator://manifest"
        )
        assert entry["manifest_id"]
        assert entry["compacted_ids"]
        assert 9101 in entry["kept_ids"]


# ---------------------------------------------------------------------------
# 待办
# ---------------------------------------------------------------------------

class TestFollowUpTodos:
    def test_wait_release_raises_todos_for_related_pages(self):
        """共享键等待释放时，状态/回放页面生成待办。"""
        now = time.time()
        _seed_shared_key("cfg_todo", "operator://todo", now, "opB", "opA")
        lifecycle.apply_ttl_cleanup("cfg_todo", "deferred", now)
        modules = {
            row["module"] for row in lifecycle.list_todos(status="待处理")
        }
        assert {"state_store", "replay_engine"} <= modules

    def test_todo_not_duplicated_on_repeat(self):
        """重复清理不会重复生成同一条待办。"""
        now = time.time()
        _seed_shared_key("cfg_todo2", "operator://todo2", now, "opB", "opA")
        lifecycle.apply_ttl_cleanup("cfg_todo2", "deferred", now)
        first = len(lifecycle.list_todos())
        lifecycle.apply_ttl_cleanup("cfg_todo2", "deferred", now + 10)
        assert len(lifecycle.list_todos()) == first
