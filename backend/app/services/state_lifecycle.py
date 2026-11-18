"""状态生命周期引擎：状态键引用索引、引用感知的TTL清理与幂等回执。

序号29的核心修复：
  - 清理前先读取状态键，把所有算子引用归并到同一个键上，不再按时间戳
    把同键的全部状态行一起停用；
  - 只有全部算子引用都过期（或显式释放）后才允许停用状态键；
  - 仍被活动检查点引用的键必须保留；
  - 重复清理不推进同一键的删除时点，相同提交只落一份批量回执；
  - 两种口径（deferred 等最后一个引用释放 / refcount 引用计数逐次递减）
    都会生成独立的状态恢复数据版本，但可消费的版本只有一个。
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from typing import Any

from app.store import store

STATE_MODULE = "state_store"
RESTORE_TABLE = "state_restore_data"
RECEIPT_TABLE = "state_cleanup_receipts"
THRESHOLD_TABLE = "state_threshold_receipts"
TODO_TABLE = "state_restore_todos"
MANIFEST_TABLE = "state_compaction_manifest"

MODE_DEFERRED = "deferred"
MODE_REFCOUNT = "refcount"

_MODE_ALIASES = {
    "deferred": MODE_DEFERRED,
    "wait_last_release": MODE_DEFERRED,
    "wait_last_ref": MODE_DEFERRED,
    "wait_all_release": MODE_DEFERRED,
    "all_expired": MODE_DEFERRED,
    "统一清理": MODE_DEFERRED,
    "等待最后一个引用释放": MODE_DEFERRED,
    "最后引用释放": MODE_DEFERRED,
    "refcount": MODE_REFCOUNT,
    "ref_count": MODE_REFCOUNT,
    "incremental": MODE_REFCOUNT,
    "引用计数": MODE_REFCOUNT,
    "逐次递减": MODE_REFCOUNT,
    "递减": MODE_REFCOUNT,
}

_LOCK = threading.RLock()

_SNAPSHOT_TABLES = (
    STATE_MODULE,
    "checkpoint_mgr",
    RECEIPT_TABLE,
    RESTORE_TABLE,
    THRESHOLD_TABLE,
    TODO_TABLE,
    MANIFEST_TABLE,
)


# ---------------------------------------------------------------------------
# 字段解析
# ---------------------------------------------------------------------------

def normalize_mode(mode: str | None) -> str:
    """把前端/调用方的清理口径归一到两个稳定值。"""
    text = str(mode or "").strip()
    if not text:
        return MODE_DEFERRED
    return _MODE_ALIASES.get(text.casefold(), _MODE_ALIASES.get(text, MODE_DEFERRED))


def _first_value(row: dict[str, Any], *fields: str) -> Any:
    for field in fields:
        if field in row and row[field] is not None:
            return row[field]
    return None


def _as_timestamp(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def parse_ttl(value: Any, default: float | None = None) -> float | None:
    """把 TTL 字段解析为秒，支持 3600 / "3600" / "30m" / "2h" / "1d"。"""
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip().lower()
    if not text:
        return default
    units = (
        ("seconds", 1.0), ("second", 1.0), ("小时", 3600.0), ("分钟", 60.0),
        ("天", 86400.0), ("secs", 1.0), ("sec", 1.0), ("minutes", 60.0),
        ("minute", 60.0), ("mins", 60.0), ("min", 60.0), ("hours", 3600.0),
        ("hour", 3600.0), ("hrs", 3600.0), ("days", 86400.0), ("day", 86400.0),
        ("秒", 1.0), ("分", 60.0),
        ("s", 1.0), ("m", 60.0), ("h", 3600.0), ("d", 86400.0),
    )
    for suffix, multiplier in units:
        if text.endswith(suffix):
            number = text[: -len(suffix)].strip()
            try:
                return float(number) * multiplier
            except ValueError:
                return default
    try:
        return float(text)
    except ValueError:
        return default


def _pattern_covers(config_pattern: str, row_pattern: str) -> bool:
    """配置的键模式能否覆盖到这一行：同键或在同一模式前缀下共享。"""
    if not config_pattern or not row_pattern:
        return False
    if config_pattern == row_pattern:
        return True
    return (
        row_pattern.startswith(config_pattern)
        and row_pattern[len(config_pattern):][:1] in ("/", ":", ".")
    )


def state_key_of(row: dict[str, Any]) -> str:
    """读取一行状态所属的逻辑状态键。"""
    for field in ("_state_key", "state_key", "状态键", "_key"):
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    pattern = row.get("状态键模式")
    if pattern not in (None, ""):
        return str(pattern)
    name = row.get("配置名称")
    if name not in (None, ""):
        return str(name)
    return f"state://{row.get('id', 'unknown')}"


def operator_of(row: dict[str, Any]) -> str:
    """读取状态行的引用算子。"""
    for field in ("_operator", "operator", "算子", "_owner"):
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    name = str(row.get("配置名称", ""))
    if name.startswith("agg_"):
        parts = name.split("_")
        if len(parts) >= 3:
            return parts[1]
    return name or "unknown"


def group_of(row: dict[str, Any]) -> str:
    """读取状态行的分组标识。"""
    for field in ("_group", "group", "分组"):
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    pattern = str(row.get("状态键模式", ""))
    if "/" in pattern:
        return pattern.rsplit("/", 1)[-1]
    return "__default__"


def last_active_of(row: dict[str, Any]) -> float | None:
    """状态行最后一次被算子写入/读取的时间。缺时间戳视为仍活跃。"""
    return _as_timestamp(_first_value(
        row, "_last_access", "_timestamp", "_updated_at", "last_access", "updated_at",
    ))


def released_at_of(row: dict[str, Any]) -> float | None:
    """引用被显式释放的时间。"""
    if row.get("_ref_released") or row.get("released"):
        return _as_timestamp(_first_value(row, "_released_at", "released_at")) or 0.0
    return _as_timestamp(_first_value(row, "_released_at", "released_at"))


def deleted_at_of(row: dict[str, Any]) -> float | None:
    """状态行被 TTL 清理删除的时间；压缩掉的重复行不算删除。"""
    value = _as_timestamp(_first_value(row, "_deleted_at", "deleted_at"))
    if value is not None:
        return value
    if row.get("_compacted"):
        return None
    if row.get("_cleaned"):
        return _as_timestamp(row.get("_cleaned_at")) or 0.0
    return None


def is_inactive_row(row: dict[str, Any]) -> bool:
    """压缩掉的重复行、已删除行都不再参与引用计数。"""
    return bool(row.get("_compacted")) or deleted_at_of(row) is not None


def reference_expired(
    row: dict[str, Any], now: float, default_ttl: float | None = None,
) -> bool:
    """按引用自身 TTL 判断是否过期。没有时间戳的行不判过期。

    row 既可以是原始状态行，也可以是 build_key_index 产出的引用摘要。
    """
    ttl = parse_ttl(
        _first_value(row, "_ttl_seconds", "ttl_seconds", "TTL时长", "ttl"),
        default_ttl,
    )
    last_active = _as_timestamp(row.get("last_active"))
    if last_active is None:
        last_active = last_active_of(row)
    if ttl is None or ttl <= 0 or last_active is None:
        return False
    return now - last_active > ttl


# ---------------------------------------------------------------------------
# 检查点引用
# ---------------------------------------------------------------------------

def _walk_state_keys(value: Any, found: set[str]) -> None:
    if isinstance(value, dict):
        for field in ("_state_key", "state_key", "状态键"):
            if value.get(field) not in (None, ""):
                found.add(str(value[field]))
        for nested in value.values():
            _walk_state_keys(nested, found)
    elif isinstance(value, (list, tuple, set)):
        for nested in value:
            _walk_state_keys(nested, found)


def checkpoint_state_keys(row: dict[str, Any]) -> set[str]:
    """读取一个检查点快照引用的状态键清单。"""
    raw = _first_value(
        row, "state_key_manifest", "state_compaction_manifest",
        "state_keys", "_state_keys", "状态键清单",
    )
    keys: set[str] = set()
    if isinstance(raw, dict):
        keys.update(str(key) for key in raw)
    elif isinstance(raw, (list, tuple, set)):
        keys.update(str(key) for key in raw)
    if not keys:
        _walk_state_keys(row.get("operator_states"), keys)
        _walk_state_keys(row.get("state"), keys)
    return keys


def active_checkpoint_rows() -> list[dict[str, Any]]:
    """仍在生效、未被裁剪的检查点。"""
    active: list[dict[str, Any]] = []
    for row in store.rows("checkpoint_mgr"):
        checkpoint_id = row.get("checkpoint_id")
        if not checkpoint_id:
            continue
        if row.get("_pruned"):
            continue
        if row.get("status") != "已启用":
            continue
        active.append(row)
    return active


def checkpoint_pins() -> dict[str, list[str]]:
    """状态键 → 引用它的活动检查点 ID。"""
    pins: dict[str, list[str]] = {}
    for row in active_checkpoint_rows():
        checkpoint_id = str(row.get("checkpoint_id") or row.get("id"))
        for key in checkpoint_state_keys(row):
            pins.setdefault(key, []).append(checkpoint_id)
    return pins


# ---------------------------------------------------------------------------
# 引用索引
# ---------------------------------------------------------------------------

def build_key_index() -> dict[str, dict[str, Any]]:
    """按状态键归并全部状态行和算子引用。"""
    pins = checkpoint_pins()
    index: dict[str, dict[str, Any]] = {}
    for row in store.rows(STATE_MODULE):
        key = state_key_of(row)
        info = index.setdefault(key, {
            "state_key": key,
            "rows": [],
            "references": [],
            "operators": [],
            "checkpoint_ids": [],
            "pinned": False,
            "groups": [],
        })
        info["rows"].append(row)
        inactive = is_inactive_row(row)
        released_at = released_at_of(row)
        info["references"].append({
            "state_id": row.get("id"),
            "operator": operator_of(row),
            "group": group_of(row),
            "aggregates": copy.deepcopy(row.get("_aggregates") or {}),
            "last_active": last_active_of(row),
            "ttl": parse_ttl(_first_value(row, "_ttl_seconds", "TTL时长"), None),
            "released": released_at is not None,
            "inactive": inactive,
            "deleted_at": deleted_at_of(row),
            "status": row.get("status", ""),
        })

    for key, info in index.items():
        pinned = pins.get(key, [])
        info["checkpoint_ids"] = list(pinned)
        info["pinned"] = bool(pinned)
        referencing = [
            ref for ref in info["references"]
            if not ref["inactive"] and not ref["released"]
        ]
        info["operators"] = sorted({ref["operator"] for ref in referencing})
        info["all_operators"] = sorted({
            ref["operator"] for ref in info["references"] if not ref["inactive"]
        })
        info["groups"] = sorted({
            ref["group"] for ref in info["references"] if not ref["inactive"]
        })
    return index


def describe_state_key_index() -> dict[str, Any]:
    """供状态页面展示的键 → 引用算子视图。"""
    index = build_key_index()
    keys = []
    for key, info in sorted(index.items()):
        references = []
        for ref in info["references"]:
            entry = dict(ref)
            entry.pop("aggregates", None)
            references.append(entry)
        keys.append({
            "state_key": key,
            "operators": info["operators"],
            "all_operators": info["all_operators"],
            "groups": info["groups"],
            "checkpoint_ids": info["checkpoint_ids"],
            "pinned": info["pinned"],
            "active_rows": sum(1 for ref in info["references"] if not ref["inactive"]),
            "references": references,
        })
    return {
        "ok": True,
        "key_count": len(keys),
        "keys": keys,
        "checkpoint_count": len(active_checkpoint_rows()),
    }


# ---------------------------------------------------------------------------
# 状态压缩清单
# ---------------------------------------------------------------------------

def latest_manifest_map() -> dict[str, dict[str, Any]]:
    """每个状态键最新的压缩清单。"""
    latest: dict[str, dict[str, Any]] = {}
    for row in store.rows(MANIFEST_TABLE):
        key = row.get("state_key")
        if not key:
            continue
        previous = latest.get(str(key))
        if previous is None or float(row.get("generated_at", 0)) >= float(
            previous.get("generated_at", 0)
        ):
            latest[str(key)] = row
    return latest


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _write_manifest(
    config_name: str,
    info: dict[str, Any],
    kept_ids: list[int],
    compacted_ids: list[int],
    now: float,
) -> dict[str, Any]:
    rows = store.rows(MANIFEST_TABLE)
    manifest_id = f"man_{_next_id(rows)}"
    entry = {
        "id": _next_id(rows),
        "manifest_id": manifest_id,
        "source": "compaction",
        "配置名称": config_name,
        "state_key": info["state_key"],
        "groups": list(info["groups"]),
        "operators": list(info["all_operators"]),
        "kept_ids": kept_ids,
        "compacted_ids": compacted_ids,
        "checkpoint_ids": list(info["checkpoint_ids"]),
        "generated_at": now,
        "version": sum(
            1 for row in rows if row.get("配置名称") == config_name
        ) + 1,
    }
    rows.append(entry)
    return entry


def compact_duplicate_states(config_name: str, now: float | None = None) -> dict[str, Any]:
    """按状态键+分组压缩同算子的重复快照，保留最新一份。

    不同算子引用同一个状态键属于共享引用，绝不压缩；被检查点引用的键
    即使存在同算子重复快照，也保留一份并写入压缩清单供检查点校验。
    """
    with _LOCK:
        now = float(now if now is not None else time.time())
        config = next(
            (row for row in store.rows(STATE_MODULE)
             if row.get("配置名称") == config_name),
            None,
        )
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}
        pattern = str(config.get("状态键模式", ""))
        candidates = [
            row for row in store.rows(STATE_MODULE)
            if not is_inactive_row(row) and (
                row.get("配置名称") == config_name
                or _pattern_covers(pattern, str(row.get("状态键模式", "")))
            )
        ]
        index = build_key_index()
        groups: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
        for row in candidates:
            key = state_key_of(row)
            if key not in index:
                continue
            groups.setdefault((key, group_of(row)), {}).setdefault(
                operator_of(row), []
            ).append(row)

        compacted = 0
        per_key: dict[str, dict[str, Any]] = {}
        for (key, _group), by_operator in groups.items():
            info = index[key]
            kept_ids: list[int] = []
            compacted_ids: list[int] = []
            for _operator, rows in by_operator.items():
                rows.sort(
                    key=lambda row: float(last_active_of(row) or 0), reverse=True,
                )
                kept_ids.append(int(rows[0].get("id", 0)))
                for duplicate in rows[1:]:
                    duplicate["status"] = "已停用"
                    duplicate["pending"] = False
                    duplicate["_compacted"] = True
                    duplicate["_compacted_at"] = now
                    compacted_ids.append(int(duplicate.get("id", 0)))
                    compacted += 1
            bucket = per_key.setdefault(key, {
                "kept_ids": [], "compacted_ids": [],
            })
            bucket["kept_ids"].extend(kept_ids)
            bucket["compacted_ids"].extend(compacted_ids)

        manifests = []
        for key, bucket in per_key.items():
            manifests.append(_write_manifest(
                config_name, index[key], sorted(set(bucket["kept_ids"])),
                sorted(set(bucket["compacted_ids"])), now,
            ))

        return {
            "ok": True,
            "config": config_name,
            "pattern": pattern,
            "groups_found": len(groups),
            "compacted": compacted,
            "manifest_id": manifests[0]["manifest_id"] if manifests else None,
            "manifests": [
                {"state_key": item["state_key"], "manifest_id": item["manifest_id"]}
                for item in manifests
            ],
            "remaining_active": sum(
                1 for row in store.rows(STATE_MODULE)
                if row.get("配置名称") == config_name and not is_inactive_row(row)
            ),
            "timestamp": now,
        }


# ---------------------------------------------------------------------------
# 事务快照：清理中途失败必须退回同一口径，不留下半批数据
# ---------------------------------------------------------------------------

def _snapshot_tables() -> dict[str, list[dict[str, Any]]]:
    return {
        name: [copy.deepcopy(row) for row in store.rows(name)]
        for name in _SNAPSHOT_TABLES
    }


def _restore_tables(snapshot: dict[str, list[dict[str, Any]]]) -> None:
    for name, rows in snapshot.items():
        table = store.rows(name)
        table.clear()
        table.extend(copy.deepcopy(rows))


# ---------------------------------------------------------------------------
# 引用感知的清理判定
# ---------------------------------------------------------------------------

def _classify_reference(ref: dict[str, Any], now: float, key_ttl: float | None) -> str:
    if ref["inactive"]:
        return "inactive"
    if ref["released"]:
        return "released"
    if ref["deleted_at"] is not None:
        return "cleaned"
    if reference_expired(ref, now, key_ttl):
        return "expired"
    return "active"


def _affected_keys(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """找出配置作用范围内的状态键，包含被其他算子共享的键。"""
    config_name = config.get("配置名称", "")
    pattern = str(config.get("状态键模式", "") or "")
    index = build_key_index()
    scoped: dict[str, dict[str, Any]] = {}
    for key, info in index.items():
        matched = False
        for row in info["rows"]:
            if row.get("配置名称") == config_name:
                matched = True
                break
            row_pattern = str(row.get("状态键模式", ""))
            if pattern and _pattern_covers(pattern, row_pattern):
                matched = True
                break
        if matched:
            scoped[key] = info
    return scoped


def _deletion_marker(row: dict[str, Any], now: float, mode: str) -> float:
    """删除时点只写一次，重复清理不再推进同一个键。"""
    existing = deleted_at_of(row)
    marker = existing if existing is not None else now
    row["_deleted_at"] = marker
    row["_cleaned_at"] = marker
    row["_cleaned"] = True
    row["_cleanup_mode"] = mode
    row["pending"] = False
    if row.get("status") != "已停用":
        row["status"] = "已停用"
    return marker


def _live_references(info: dict[str, Any], now: float, ttl: float | None) -> list[dict[str, Any]]:
    return [
        ref for ref in info["references"]
        if _classify_reference(ref, now, ttl) in ("active", "expired")
    ]


def group_payload(info: dict[str, Any], now: float, ttl: float | None) -> list[dict[str, Any]]:
    """状态恢复数据里的分组：只保留仍有引用（未删除、未释放）的分组。"""
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for ref in info["references"]:
        state = _classify_reference(ref, now, ttl)
        if state in ("inactive", "released", "cleaned"):
            continue
        key = (ref["operator"], ref["group"])
        entry = groups.get(key)
        if entry is None:
            entry = {
                "operator": ref["operator"],
                "group": ref["group"],
                "state_id": ref["state_id"],
                "aggregates": copy.deepcopy(ref["aggregates"]),
                "last_active": ref["last_active"],
                "reference_state": state,
            }
            groups[key] = entry
        elif float(ref["last_active"] or 0) > float(entry["last_active"] or 0):
            entry["aggregates"] = copy.deepcopy(ref["aggregates"])
            entry["state_id"] = ref["state_id"]
            entry["last_active"] = ref["last_active"]
            entry["reference_state"] = state
    return list(groups.values())


def evaluate_cleanup_plan(
    config_name: str,
    mode: str | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """只读判定：不写任何状态，用于阈值核对页和清理预演。"""
    with _LOCK:
        now = float(now if now is not None else time.time())
        mode = normalize_mode(mode)
        config = next(
            (row for row in store.rows(STATE_MODULE)
             if row.get("配置名称") == config_name),
            None,
        )
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}
        default_ttl = parse_ttl(config.get("TTL时长"), 3600.0)
        keys = _affected_keys(config)
        plan_keys = []
        for key, info in sorted(keys.items()):
            live = _live_references(info, now, default_ttl)
            expired = [
                ref for ref in live
                if _classify_reference(ref, now, default_ttl) == "expired"
            ]
            active = [ref for ref in live if ref not in expired]
            if info["pinned"]:
                action = "hold_checkpoint"
            elif not live:
                action = "noop"
            elif not expired:
                action = "active"
            elif mode == MODE_REFCOUNT:
                action = "refcount" if active else "cleanup"
            elif active:
                action = "wait_last_release"
            else:
                action = "cleanup"
            plan_keys.append({
                "state_key": key,
                "action": action,
                "pinned": info["pinned"],
                "checkpoint_ids": info["checkpoint_ids"],
                "operators": sorted({ref["operator"] for ref in live}),
                "expired_operators": sorted({ref["operator"] for ref in expired}),
                "live_operators": sorted({ref["operator"] for ref in active}),
                "groups": [ref["group"] for ref in live],
                "expired_state_ids": [ref["state_id"] for ref in expired],
                "kept_state_ids": [ref["state_id"] for ref in active],
            })
        return {"ok": True, "config": config_name, "mode": mode, "now": now, "keys": plan_keys}


def apply_ttl_cleanup(
    config_name: str,
    mode: str | None = None,
    now: float | None = None,
    *,
    dry_run: bool = False,
    request_id: str | None = None,
) -> dict[str, Any]:
    """序号29的清理入口：读取状态键 → 判断引用 → 清理全部过期引用。

    deferred: 同键里还有活动算子引用时整键保留，等最后一个引用释放后统一清理；
    refcount: 逐条递减已过期的引用，剩最后一个活动引用时键仍然可用。
    """
    with _LOCK:
        now = float(now if now is not None else time.time())
        mode = normalize_mode(mode)
        config = next(
            (row for row in store.rows(STATE_MODULE)
             if row.get("配置名称") == config_name),
            None,
        )
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}
        default_ttl = parse_ttl(config.get("TTL时长"), 3600.0)
        keys = _affected_keys(config)
        if dry_run:
            return {"ok": True, "dry_run": True, **evaluate_cleanup_plan(
                config_name, mode, now,
            )}

        snapshot = _snapshot_tables()
        try:
            decisions: list[dict[str, Any]] = []
            cleaned_refs: list[dict[str, Any]] = []
            deleted_keys: list[dict[str, Any]] = []
            retained_keys: list[dict[str, Any]] = []
            held_keys: list[dict[str, Any]] = []
            decremented: list[dict[str, Any]] = []
            version_rows: list[dict[str, Any]] = []

            for key, info in sorted(keys.items()):
                live = _live_references(info, now, default_ttl)
                expired = [
                    ref for ref in live
                    if _classify_reference(ref, now, default_ttl) == "expired"
                ]
                active = [ref for ref in live if ref not in expired]
                decision: dict[str, Any] = {
                    "state_key": key,
                    "mode": mode,
                    "pinned": info["pinned"],
                    "checkpoint_ids": info["checkpoint_ids"],
                    "live_operators": sorted({ref["operator"] for ref in active}),
                    "expired_operators": sorted({ref["operator"] for ref in expired}),
                }

                if info["pinned"]:
                    decision["action"] = "hold_checkpoint"
                    decision["kept_state_ids"] = [
                        ref["state_id"] for ref in live
                    ]
                    held_keys.append(decision)
                    retained_keys.append(decision)
                    # 已有可消费版本时不再新增版本，避免检查点反复切换版本
                    if active_restore_version(key) is None:
                        version_rows.append(_build_version(
                            key, info, now, default_ttl, mode, decision,
                        ))
                    decisions.append(decision)
                    continue

                if mode == MODE_REFCOUNT:
                    if not expired:
                        decision["action"] = "noop" if not live else "active"
                        decision["kept_state_ids"] = [ref["state_id"] for ref in live]
                        if live:
                            retained_keys.append(decision)
                        decisions.append(decision)
                        continue
                    deleted_ids: list[int | None] = []
                    for ref in expired:
                        row = next(
                            (item for item in info["rows"]
                             if item.get("id") == ref["state_id"]),
                            None,
                        )
                        if row is None:
                            continue
                        marker = _deletion_marker(row, now, mode)
                        row["_cleanup_receipt"] = f"pending:{config_name}:{mode}"
                        cleaned_refs.append({
                            "state_key": key,
                            "state_id": ref["state_id"],
                            "operator": ref["operator"],
                            "group": ref["group"],
                            "deleted_at": marker,
                        })
                        deleted_ids.append(ref["state_id"])
                        decremented.append({
                            "state_key": key,
                            "operator": ref["operator"],
                            "group": ref["group"],
                            "ref_count_before": len(live),
                            "ref_count_after": len(active),
                        })
                    decision["action"] = "cleanup" if not active else "refcount"
                    decision["deleted_state_ids"] = deleted_ids
                    decision["kept_state_ids"] = [ref["state_id"] for ref in active]
                    if active:
                        retained_keys.append(decision)
                    else:
                        deleted_keys.append(decision)
                    version_rows.append(_build_version(
                        key, info, now, default_ttl, mode, decision,
                    ))
                    decisions.append(decision)
                    continue

                # deferred：全部引用都过期才允许停用整个键
                if not expired:
                    decision["action"] = "noop" if not live else "active"
                    decision["kept_state_ids"] = [ref["state_id"] for ref in live]
                    if live:
                        retained_keys.append(decision)
                    decisions.append(decision)
                    continue
                if active:
                    decision["action"] = "wait_last_release"
                    decision["kept_state_ids"] = [ref["state_id"] for ref in live]
                    decision["held_by"] = decision["live_operators"]
                    decision["expired_state_ids"] = [ref["state_id"] for ref in expired]
                    held_keys.append(decision)
                    retained_keys.append(decision)
                    version_rows.append(_build_version(
                        key, info, now, default_ttl, mode, decision,
                    ))
                    decisions.append(decision)
                    continue
                for ref in expired:
                    row = next(
                        (item for item in info["rows"]
                         if item.get("id") == ref["state_id"]),
                        None,
                    )
                    if row is None:
                        continue
                    marker = _deletion_marker(row, now, mode)
                    row["_cleanup_receipt"] = f"pending:{config_name}:{mode}"
                    cleaned_refs.append({
                        "state_key": key,
                        "state_id": ref["state_id"],
                        "operator": ref["operator"],
                        "group": ref["group"],
                        "deleted_at": marker,
                    })
                decision["action"] = "cleanup"
                decision["deleted_state_ids"] = [ref["state_id"] for ref in expired]
                decision["kept_state_ids"] = []
                deleted_keys.append(decision)
                version_rows.append(_build_version(
                    key, info, now, default_ttl, mode, decision,
                ))
                decisions.append(decision)

            fingerprint = _cleanup_fingerprint(keys)

            # 幂等：显式 request_id 相同，或本轮没有任何引用状态变化时，
            # 直接返回已有回执，不重复落记录、不推进删除时点。
            receipt = None
            if request_id:
                receipt = _find_duplicate_receipt(
                    config_name, mode, request_id, fingerprint,
                )
            elif not cleaned_refs and not version_rows:
                receipt = _latest_receipt(config_name, mode)
            if receipt is not None:
                # 并发重试按当前标准接收整组结果：回执里的决策、保留键和版本
                # 原样返回，但不重复写库、不推进删除时点。
                return {
                    "ok": True,
                    "config": config_name,
                    "mode": mode,
                    "duplicate": True,
                    "receipt_id": receipt["receipt_id"],
                    "message": "同一批清理已提交，返回原回执",
                    "cleaned": 0,
                    "cleaned_total": len(receipt.get("cleaned_state_ids", [])),
                    "cleaned_keys": list(receipt.get("deleted_keys", [])),
                    "retained_keys": list(receipt.get("retained_keys", [])),
                    "held_keys": list(receipt.get("held_keys", [])),
                    "decremented": copy.deepcopy(receipt.get("decrements", [])),
                    "decisions": copy.deepcopy(receipt.get("decisions", [])),
                    "version_ids": list(receipt.get("version_ids", [])),
                    "versions": list(receipt.get("version_ids", [])),
                    "timestamp": receipt.get("created_at", now),
                }

            for ref in cleaned_refs:
                if ref.get("state_id") is None:
                    continue
                row = next(
                    (item for item in store.rows(STATE_MODULE)
                     if item.get("id") == ref["state_id"]),
                    None,
                )
                if row is not None:
                    row["_cleanup_receipt"] = f"pending:{config_name}"

            versions = _write_restore_versions(
                config_name, mode, now, version_rows, None,
            )
            receipt_id = f"rcpt_{_next_id(store.rows(RECEIPT_TABLE))}"
            for ref in cleaned_refs:
                if ref.get("state_id") is None:
                    continue
                row = next(
                    (item for item in store.rows(STATE_MODULE)
                     if item.get("id") == ref["state_id"]),
                    None,
                )
                if row is not None:
                    row["_cleanup_receipt"] = receipt_id
            _raise_todos(config_name, mode, decisions, versions, receipt_id, now)

            receipt_row = {
                "id": _next_id(store.rows(RECEIPT_TABLE)),
                "receipt_id": receipt_id,
                "config": config_name,
                "mode": mode,
                "request_id": request_id,
                "fingerprint": fingerprint,
                "status": "已形成",
                "decisions": copy.deepcopy(decisions),
                "cleaned_state_ids": [ref["state_id"] for ref in cleaned_refs],
                "decrements": copy.deepcopy(decremented),
                "deleted_keys": [item["state_key"] for item in deleted_keys],
                "retained_keys": [item["state_key"] for item in retained_keys],
                "held_keys": [item["state_key"] for item in held_keys],
                "checkpoint_ids": sorted({
                    cid for item in decisions for cid in item["checkpoint_ids"]
                }),
                "version_ids": [item["restore_id"] for item in versions],
                "created_at": now,
            }
            store.rows(RECEIPT_TABLE).append(receipt_row)

            return {
                "ok": True,
                "duplicate": False,
                "receipt_id": receipt_id,
                "config": config_name,
                "mode": mode,
                "cleaned": len(cleaned_refs),
                "cleaned_keys": [item["state_key"] for item in deleted_keys],
                "retained_keys": [item["state_key"] for item in retained_keys],
                "held_keys": [item["state_key"] for item in held_keys],
                "decremented": decremented,
                "decisions": decisions,
                "versions": versions,
                "version_ids": receipt_row["version_ids"],
                "timestamp": now,
            }
        except Exception as exc:  # 事务失败：退回同一口径的基线
            _restore_tables(snapshot)
            return {
                "ok": False,
                "rolled_back": True,
                "config": config_name,
                "mode": mode,
                "error": f"清理事务失败，已回退：{exc}",
                "baseline_preserved": True,
            }


def _cleanup_fingerprint(keys: dict[str, dict[str, Any]]) -> str:
    """按清理后的键状态生成指纹：同一批引用状态只生成一份回执。"""
    parts = []
    for key, info in sorted(keys.items()):
        refs = sorted(
            (
                str(ref["operator"]), str(ref["group"]), str(ref["state_id"]),
                ref["deleted_at"], ref["released"], ref["inactive"],
            )
            for ref in info["references"]
        )
        parts.append((key, tuple(refs)))
    return hashlib.md5(repr(parts).encode()).hexdigest()


def _latest_receipt(config_name: str, mode: str) -> dict[str, Any] | None:
    for row in reversed(store.rows(RECEIPT_TABLE)):
        if row.get("config") == config_name and row.get("mode") == mode:
            return row
    return None


def _find_duplicate_receipt(
    config_name: str, mode: str, request_id: str | None, fingerprint: str,
) -> dict[str, Any] | None:
    receipts = store.rows(RECEIPT_TABLE)
    if request_id:
        for row in reversed(receipts):
            if row.get("request_id") == request_id and row.get("config") == config_name:
                return row
    for row in reversed(receipts):
        if (
            row.get("config") == config_name
            and row.get("mode") == mode
            and row.get("fingerprint") == fingerprint
        ):
            return row
    return None


# ---------------------------------------------------------------------------
# 状态恢复数据（版本）
# ---------------------------------------------------------------------------

def _rebind_checkpoints(
    state_key: str,
    superseded: list[dict[str, Any]],
    restore_id: str,
) -> None:
    """检查点跟随状态键的活动版本，同时保留来源版本用于追溯。"""
    superseded_ids = {
        str(row.get("restore_id")) for row in superseded if row.get("restore_id")
    }
    for row in store.rows("checkpoint_mgr"):
        if not row.get("checkpoint_id"):
            continue
        current = str(row.get("restore_id") or "")
        references_key = state_key in checkpoint_state_keys(row)
        if current in superseded_ids or (not current and references_key):
            row["source_version"] = current or row.get("source_version")
            row["restore_id"] = restore_id


def _write_restore_versions(
    config_name: str,
    mode: str,
    now: float,
    version_rows: list[dict[str, Any]],
    receipt_id: str | None,
) -> list[dict[str, Any]]:
    table = store.rows(RESTORE_TABLE)
    written: list[dict[str, Any]] = []
    fresh_index = build_key_index()
    for sheet in version_rows:
        key = sheet["state_key"]
        fresh = fresh_index.get(key)
        ttl = parse_ttl(_first_value(
            (fresh or {}).get("rows", [{}])[0] if fresh else {}, "TTL时长",
        ), None)
        if sheet.get("keep_groups") and fresh is not None:
            groups = group_payload(fresh, now, ttl)
        else:
            groups = []
        previous = [
            row for row in table
            if row.get("state_key") == key and row.get("status") == "活动"
        ]
        previous.sort(key=lambda row: float(row.get("created_at", 0)), reverse=True)
        source_version = (
            previous[0].get("restore_id") if previous else sheet.get("source_version")
        )
        version = 1 + max(
            (int(row.get("version", 0)) for row in table
             if row.get("state_key") == key),
            default=0,
        )
        restore_id = f"restore://{key}/{mode}/v{version}"
        entry = {
            "id": _next_id(table),
            "restore_id": restore_id,
            "state_key": key,
            "config": config_name,
            "mode": mode,
            "version": version,
            "status": "活动",
            "source_version": source_version,
            "source_manifest_id": sheet.get("source_manifest_id"),
            "groups": copy.deepcopy(groups),
            "group_count": len(groups),
            "operators": sorted({item["operator"] for item in groups}),
            "checkpoint_ids": list(sheet.get("checkpoint_ids", [])),
            "retained": bool(sheet.get("retained")),
            "receipt_id": receipt_id,
            "created_at": now,
            "generated_from": "state_cleanup",
        }
        for old in previous:
            old["status"] = "已归档"
            old["archived_at"] = now
            old["_archived_by"] = restore_id
        table.append(entry)
        _rebind_checkpoints(key, previous, restore_id)
        written.append({
            "restore_id": restore_id,
            "state_key": key,
            "mode": mode,
            "version": version,
            "source_version": source_version,
            "group_count": entry["group_count"],
            "status": "活动",
        })
        for old in previous:
            written.append({
                "restore_id": old.get("restore_id"),
                "state_key": key,
                "mode": old.get("mode"),
                "version": old.get("version"),
                "status": "已归档",
            })
    return written


def _build_version(
    key: str,
    info: dict[str, Any],
    now: float,
    ttl: float | None,
    mode: str,
    decision: dict[str, Any],
    groups: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """登记一个待写入的状态恢复数据版本（分组在写入时按最新引用计算）。"""
    manifest = latest_manifest_map().get(key)
    return {
        "state_key": key,
        "keep_groups": decision.get("action") != "cleanup",
        "retained": decision.get("action") != "cleanup",
        "checkpoint_ids": info["checkpoint_ids"],
        "source_manifest_id": manifest.get("manifest_id") if manifest else None,
        "source_version": None,
    }


def list_restore_data(
    state_key: str | None = None,
    mode: str | None = None,
    active_only: bool = False,
) -> list[dict[str, Any]]:
    """列出状态恢复数据版本；每个状态键最多一条活动版本。"""
    rows = []
    for row in store.rows(RESTORE_TABLE):
        if state_key and row.get("state_key") != state_key:
            continue
        if mode and row.get("mode") != normalize_mode(mode):
            continue
        if active_only and row.get("status") != "活动":
            continue
        rows.append(copy.deepcopy(row))
    rows.sort(key=lambda row: (str(row.get("state_key")), int(row.get("version", 0))))
    return rows


def get_restore_version(restore_id: str) -> dict[str, Any] | None:
    for row in store.rows(RESTORE_TABLE):
        if row.get("restore_id") == restore_id:
            return row
    return None


def active_restore_version(state_key: str) -> dict[str, Any] | None:
    active = None
    for row in store.rows(RESTORE_TABLE):
        if row.get("state_key") == state_key and row.get("status") == "活动":
            if active is None or int(row.get("version", 0)) >= int(active.get("version", 0)):
                active = row
    return active


def list_receipts(config_name: str | None = None) -> list[dict[str, Any]]:
    rows = [
        row for row in store.rows(RECEIPT_TABLE)
        if config_name is None or row.get("config") == config_name
    ]
    return [copy.deepcopy(row) for row in sorted(
        rows, key=lambda row: float(row.get("created_at", 0)), reverse=True,
    )]


# ---------------------------------------------------------------------------
# 待办
# ---------------------------------------------------------------------------

def _raise_todos(
    config_name: str,
    mode: str,
    decisions: list[dict[str, Any]],
    versions: list[dict[str, Any]],
    receipt_id: str,
    now: float,
) -> None:
    todos = store.rows(TODO_TABLE)

    def upsert(kind: str, module: str, state_key: str | None, message: str) -> None:
        token = f"{kind}:{module}:{config_name}:{mode}:{state_key or '-'}"
        for row in todos:
            if row.get("token") == token:
                row["updated_at"] = now
                return
        todos.append({
            "id": _next_id(todos),
            "token": token,
            "kind": kind,
            "module": module,
            "page": module,
            "config": config_name,
            "mode": mode,
            "state_key": state_key,
            "message": message,
            "status": "待处理",
            "receipt_id": receipt_id,
            "created_at": now,
            "updated_at": now,
        })

    for decision in decisions:
        if decision["action"] == "wait_last_release":
            upsert(
                "wait_last_release", STATE_MODULE, decision["state_key"],
                "共享状态键仍被活动算子引用，等待最后一个引用释放后再统一清理",
            )
            upsert(
                "verify_shared_key_not_removed", "replay_engine", decision["state_key"],
                "回放前确认共享状态键没有被提前移除",
            )
        elif decision["action"] == "hold_checkpoint":
            upsert(
                "checkpoint_pin_review", "checkpoint_mgr", decision["state_key"],
                "状态键仍被检查点引用，保留并在检查点详情页核对压缩清单",
            )
        elif decision["action"] in ("cleanup", "refcount") and decision.get("deleted_state_ids"):
            upsert(
                "restore_version_dropped", "restore_plan", decision["state_key"],
                "状态键已清理，恢复方案需按状态恢复数据当前版本重新核对分组",
            )


def list_todos(module: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
    rows = store.rows(TODO_TABLE)
    result = [
        copy.deepcopy(row) for row in rows
        if (module is None or row.get("module") == module)
        and (status is None or row.get("status") == status)
    ]
    result.sort(key=lambda row: float(row.get("created_at", 0)), reverse=True)
    return result


def resolve_todo(todo_id: int) -> dict[str, Any]:
    for row in store.rows(TODO_TABLE):
        if int(row.get("id", 0)) == int(todo_id):
            row["status"] = "已处理"
            row["updated_at"] = time.time()
            return {"ok": True, "todo": row}
    return {"ok": False, "error": f"待办 {todo_id} 不存在"}


# ---------------------------------------------------------------------------
# 阈值核对 / 恢复数据校验
# ---------------------------------------------------------------------------

def _scope_keys(config: dict[str, Any] | None, scope: str | None) -> list[str]:
    index = build_key_index()
    keys = list(index)
    if config is not None:
        keys = list(_affected_keys(config))
    if scope:
        keys = [key for key in keys if scope in key]
    return sorted(keys)


def verify_restore_threshold(
    config_name: str,
    mode: str | None = None,
    *,
    scope: str | None = None,
    min_groups: int = 0,
    require_checkpoint_alignment: bool = True,
) -> dict[str, Any]:
    """阈值核对：状态恢复数据必须保留全部仍有引用的分组。"""
    config = next(
        (row for row in store.rows(STATE_MODULE)
         if row.get("配置名称") == config_name),
        None,
    )
    if config is None:
        return {"ok": False, "error": f"状态配置「{config_name}」不存在"}
    mode = normalize_mode(mode)
    index = build_key_index()
    keys = _scope_keys(config, scope)
    checks = []
    for key in keys:
        info = index[key]
        groups = group_payload(info, time.time(), parse_ttl(config.get("TTL时长"), 3600.0))
        version = active_restore_version(key)
        retained = {
            (item["operator"], item["group"])
            for item in (version or {}).get("groups", [])
        }
        expected = {(item["operator"], item["group"]) for item in groups}
        missing = sorted(expected - retained)
        extra = sorted(retained - expected)
        version_ok = version is not None and not missing
        checkpoint_ok = (
            not require_checkpoint_alignment
            or not info["checkpoint_ids"]
            or (version is not None and not set(info["checkpoint_ids"]) - set(
                version.get("checkpoint_ids", [])
            ))
        )
        passed = (
            version_ok
            and checkpoint_ok
            and len(expected) >= min_groups
            and (not info["pinned"] or version.get("retained", False))
        )
        checks.append({
            "state_key": key,
            "mode": mode,
            "expected_groups": len(expected),
            "retained_groups": len(retained),
            "missing_groups": [
                {"operator": operator, "group": group}
                for operator, group in missing
            ],
            "extra_groups": [
                {"operator": operator, "group": group}
                for operator, group in extra
            ],
            "restore_id": version.get("restore_id") if version else None,
            "source_version": version.get("source_version") if version else None,
            "pinned": info["pinned"],
            "passed": passed,
            "rule_hits": _threshold_rule_hits(
                version_ok, checkpoint_ok, len(expected), min_groups, info,
            ),
        })
    all_passed = len(checks) > 0 and all(item["passed"] for item in checks)
    fingerprint = hashlib.md5(
        repr(sorted(
            (item["state_key"], item["passed"], item["retained_groups"])
            for item in checks
        )).encode()
    ).hexdigest()
    receipt = None
    for row in reversed(store.rows(THRESHOLD_TABLE)):
        if row.get("fingerprint") == fingerprint and row.get("mode") == mode:
            receipt = row
            break
    if receipt is None:
        receipt = {
            "id": _next_id(store.rows(THRESHOLD_TABLE)),
            "receipt_id": f"thr_{_next_id(store.rows(THRESHOLD_TABLE))}",
            "config": config_name,
            "mode": mode,
            "scope": scope,
            "min_groups": min_groups,
            "expected_groups": sum(item["expected_groups"] for item in checks),
            "retained_groups": sum(item["retained_groups"] for item in checks),
            "missing_groups": [
                {"state_key": item["state_key"], **group}
                for item in checks for group in item["missing_groups"]
            ],
            "passed": all_passed,
            "fingerprint": fingerprint,
            "created_at": time.time(),
        }
        store.rows(THRESHOLD_TABLE).append(receipt)
    return {
        "ok": True,
        "config": config_name,
        "mode": mode,
        "scope": scope,
        "min_groups": min_groups,
        "passed": all_passed,
        "receipt": copy.deepcopy(receipt),
        "keys": checks,
        "condition_range": {
            "scope": scope or config.get("状态键模式", ""),
            "min_groups": min_groups,
            "require_checkpoint_alignment": require_checkpoint_alignment,
            "mode": mode,
        },
    }


def _threshold_rule_hits(
    version_ok: bool,
    checkpoint_ok: bool,
    group_count: int,
    min_groups: int,
    info: dict[str, Any],
) -> list[dict[str, Any]]:
    hits = []
    if not version_ok:
        hits.append({"rule": "restore_groups_retained", "hit": True})
    if not checkpoint_ok:
        hits.append({"rule": "checkpoint_alignment", "hit": True})
    if group_count < min_groups:
        hits.append({"rule": "min_group_scope", "hit": True})
    if info["pinned"]:
        hits.append({"rule": "checkpoint_key_pinned", "hit": True})
    return hits


def validate_restore_data(
    restore_id: str,
    checkpoint_id: str | None = None,
) -> dict[str, Any]:
    """检查点 / 回放校验入口：按压缩清单和活动引用验证状态恢复数据。"""
    version = get_restore_version(restore_id)
    if version is None:
        return {
            "ok": False,
            "valid": False,
            "error": f"状态恢复数据版本「{restore_id}」不存在",
            "missing_keys": [],
            "rule_hits": [{"rule": "restore_version_exists", "hit": True}],
            "source_version": None,
        }
    key = version.get("state_key", "")
    info = build_key_index().get(key)
    retained_groups = {
        (item["operator"], item["group"]) for item in version.get("groups", [])
    }

    if info is None:
        # 状态键已经完全清理：只有分组为空的历史版本仍然自洽
        valid = not retained_groups
        return {
            "ok": valid,
            "valid": valid,
            "restore_id": restore_id,
            "state_key": key,
            "missing_keys": [key] if not valid else [],
            "missing_groups": [],
            "source_version": version.get("source_version"),
            "source_manifest_id": version.get("source_manifest_id"),
            "rule_hits": [{"rule": "state_key_exists", "hit": not valid}],
            "version_status": version.get("status"),
        }

    ttl = parse_ttl(_first_value(
        info["rows"][0] if info["rows"] else {}, "TTL时长",
    ), None)
    expected = {(item["operator"], item["group"]) for item in group_payload(
        info, time.time(), ttl,
    )}

    checkpoint_ids = set(version.get("checkpoint_ids", []))
    if checkpoint_id:
        checkpoint_ids.add(str(checkpoint_id))
    pinned_by_checkpoint = False
    for row in active_checkpoint_rows():
        cid = str(row.get("checkpoint_id") or row.get("id"))
        if cid in checkpoint_ids and key in checkpoint_state_keys(row):
            pinned_by_checkpoint = True
            break

    manifest = latest_manifest_map().get(key)
    manifest_keys = set()
    if manifest is not None:
        for state_id in manifest.get("kept_ids", []) + manifest.get("compacted_ids", []):
            row = next(
                (item for item in info["rows"] if item.get("id") == state_id),
                None,
            )
            if row is not None:
                manifest_keys.add(state_key_of(row))
    else:
        # 没有压缩清单时至少要能按活动引用回推这个键
        manifest_keys.add(key)

    # 状态键是否还有活动引用：全部引用被删除/释放后，���些键就不该再被消费
    live_references = _live_references(info, time.time(), ttl)
    key_still_live = bool(live_references)

    missing_groups = sorted(expected - retained_groups)
    missing_keys = set()
    if retained_groups and not key_still_live:
        missing_keys.add(key)
    if retained_groups:
        missing_keys |= {key} - manifest_keys
    missing_keys = sorted(missing_keys)
    valid = not missing_groups and not missing_keys
    hits = []
    if missing_keys:
        hits.append({"rule": "manifest_key_present", "hit": True})
    if retained_groups and not key_still_live:
        hits.append({"rule": "state_key_still_referenced", "hit": True})
    if missing_groups:
        hits.append({"rule": "active_reference_group_present", "hit": True})
    if pinned_by_checkpoint:
        hits.append({"rule": "checkpoint_key_referenced", "hit": True})
    if version.get("status") != "活动":
        hits.append({"rule": "version_active", "hit": True})
        valid = False

    return {
        "ok": valid,
        "valid": valid,
        "restore_id": restore_id,
        "state_key": key,
        "mode": version.get("mode"),
        "version": version.get("version"),
        "version_status": version.get("status"),
        "source_version": version.get("source_version"),
        "source_manifest_id": version.get("source_manifest_id"),
        "manifest_keys": sorted(manifest_keys),
        "expected_groups": len(expected),
        "retained_groups": len(retained_groups),
        "missing_keys": missing_keys,
        "missing_groups": [
            {"operator": operator, "group": group}
            for operator, group in missing_groups
        ],
        "checkpoint_ids": sorted(checkpoint_ids),
        "pinned_by_checkpoint": pinned_by_checkpoint,
        "rule_hits": hits,
    }


def build_checkpoint_manifest(checkpoint_id: str) -> dict[str, Any]:
    """为检查点详情页生成状态压缩清单 + 活动引用视图。"""
    snapshot = next(
        (row for row in store.rows("checkpoint_mgr")
         if str(row.get("checkpoint_id") or row.get("id")) == str(checkpoint_id)),
        None,
    )
    if snapshot is None:
        return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}
    keys = sorted(checkpoint_state_keys(snapshot))
    index = build_key_index()
    entries = []
    for key in keys:
        manifest = latest_manifest_map().get(key)
        info = index.get(key)
        entries.append({
            "state_key": key,
            "manifest_id": manifest.get("manifest_id") if manifest else None,
            "kept_ids": manifest.get("kept_ids", []) if manifest else [],
            "compacted_ids": manifest.get("compacted_ids", []) if manifest else [],
            "groups": (info or {}).get("groups", []),
            "operators": (info or {}).get("all_operators", []),
            "pinned": bool(info and info["pinned"]),
        })
    return {
        "ok": True,
        "checkpoint_id": str(snapshot.get("checkpoint_id") or snapshot.get("id")),
        "config": snapshot.get("配置名称", ""),
        "state_key_manifest": keys,
        "entries": entries,
    }

