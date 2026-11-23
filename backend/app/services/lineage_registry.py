"""共用血缘登记器：目录同步与快照生成共用同一套写入、回查和冲突校验过程。

一次「血缘登记」可以包含多条关系，它们共享一个批次编号（batch_id）。批次编号按
来源模块命名空间隔离，目录同步的批次号与快照生成器的批次号不会互相串用。

登记过程具备以下语义：

* 幂等续写：同一批次重复登记只补齐缺失的关系，已写入的关系保持原样；
* 组完整性：批次只有在所有预期关系都落库后才算齐全，回查时不会把部分记录
  当作整组生效；
* 对象范围冲突：用同一批次编号提交与首次登记对象范围不同的关系时，返回冲突
  位置和差异，且不修改任何已登记记录；
* 按来源定位：快照生成器可以用自己的任务编号唯一定位到它登记的那一条来源。

底层仍然写入 ``lineage_graph`` 模块，保持原有字段格式不变；批次索引、来源模块
等登记元数据保存在本登记器内，不污染业务列表。
"""
from __future__ import annotations

from datetime import datetime, timezone
from threading import RLock
from typing import Any, Iterable, Mapping, Sequence

from app.store import store

SOURCE_CATALOG = "catalog_sync"
SOURCE_SNAPSHOT = "snapshot_creator"

# 目录同步一次登记三条来源关系，这是该批次的固定对象范围。
CATALOG_LINEAGE_COUNT = 3

# 血缘关系在 lineage_graph 中的原始字段；顺序与原实现完全一致。
LINEAGE_FIELDS = ("目标表", "来源表列表", "转换逻辑", "中间层级", "血缘深度")

# 兼容英文键名，方便调用方直接传领域对象的字段副本。
_FIELD_ALIASES: dict[str, str] = {
    "目标表": "目标表",
    "target_table": "目标表",
    "target": "目标表",
    "来源表列表": "来源表列表",
    "source_tables": "来源表列表",
    "source_table_list": "来源表列表",
    "sources": "来源表列表",
    "source": "来源表列表",
    "转换逻辑": "转换逻辑",
    "transform": "转换逻辑",
    "transform_logic": "转换逻辑",
    "中间层级": "中间层级",
    "intermediate_layer": "中间层级",
    "layer": "中间层级",
    "血缘深度": "血缘深度",
    "lineage_depth": "血缘深度",
    "depth": "血缘深度",
}

_DEFAULTS = {
    "中间层级": "1",
    "血缘深度": "1",
}

_INTERRUPT_OPTIONS = (
    "limit", "max_items", "max_records", "stop_after",
    "interrupt_after", "records_limit", "only_first", "first_only",
)

# 批次清单与行元数据都只存在于进程内，与 store 的内存生命周期一致。
_batches: dict[str, dict[str, Any]] = {}
_row_meta: dict[int, dict[str, Any]] = {}
_lock = RLock()


def batch_key(source: str, owner_id: object) -> str:
    """返回来源模块命名空间下的批次编号。

    目录同步任务 1 与快照任务 1 会得到两个不同的批次编号，不能互相回查。
    """
    return f"{source}:{owner_id}"


def _split_sources(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts = [str(item).strip() for item in value]
    else:
        parts = [part.strip() for part in str(value).split(",")]
    return [part for part in parts if part]


def _pick(record: Mapping[str, Any], field: str, default: Any = "") -> Any:
    if field in record:
        return record[field]
    for alias, canonical in _FIELD_ALIASES.items():
        if canonical == field and alias in record:
            return record[alias]
    return default


def _source_values(record: Mapping[str, Any]) -> Any:
    """来源表字段可能是列表、逗号分隔字符串或下划线命名，统一取出原始值。"""
    if "来源表列表" in record:
        return record["来源表列表"]
    if "source_tables" in record:
        return record["source_tables"]
    for key in ("来源表", "source_table", "source"):
        if key in record:
            return record[key]
    sources = record.get("sources")
    if isinstance(sources, (list, tuple, set)):
        return list(sources)
    if sources is not None:
        return sources
    for key in record:
        if isinstance(key, str) and key.lower() in ("source_table_list", "source_list"):
            return record[key]
    return ""


def _normalize_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """把调用方传入的关系整理为 lineage_graph 的原始字段格式。"""
    if not isinstance(record, Mapping):
        raise TypeError("血缘关系必须是字段字典")
    normalized: dict[str, Any] = {}
    for field in LINEAGE_FIELDS:
        if field == "来源表列表":
            value = _source_values(record)
        else:
            value = _pick(record, field, _DEFAULTS.get(field, ""))
        if field == "来源表列表" and isinstance(value, (list, tuple, set)):
            value = ", ".join(str(item).strip() for item in value if str(item).strip())
        normalized[field] = "" if value is None else str(value)
    # 快照来源需要携带任务编号，便于按编号定位；它不是批次一致性字段。
    if isinstance(record.get("snapshot_id"), (int, str)) and str(record["snapshot_id"]).strip():
        normalized["snapshot_id"] = record["snapshot_id"]
    return normalized


def _normalize_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """接受 ``[record]``、``{index: record}`` 或单条 record，统一成列表。"""
    if isinstance(records, Mapping):
        if "目标表" in records or "target_table" in records or "target" in records:
            return [_normalize_record(records)]
        return [
            _normalize_record(records[key])
            for key in sorted(records, key=lambda item: int(item))
        ]
    return [_normalize_record(record) for record in records]


def _object_scope(record: Mapping[str, Any]) -> tuple[str, tuple[str, ...]]:
    """对象范围：目标表 + 来源表集合。这是判断批次冲突的核心口径。"""
    return (
        str(record.get("目标表", "")).strip(),
        tuple(sorted(_split_sources(record.get("来源表列表", "")))),
    )


def _scope_diff(expected: Mapping[str, Any], provided: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    expected_scope = _object_scope(expected)
    provided_scope = _object_scope(provided)
    fields: list[str] = []
    reasons: list[str] = []
    if expected_scope[0] != provided_scope[0]:
        fields.append("目标表")
        reasons.append("目标表不一致")
    if expected_scope[1] != provided_scope[1]:
        fields.append("来源表列表")
        reasons.append("来源表范围不一致")
    if str(expected.get("转换逻辑", "")) != str(provided.get("转换逻辑", "")):
        fields.append("转换逻辑")
        reasons.append("转换逻辑不一致")
    for field in ("中间层级", "血缘深度"):
        if str(expected.get(field, "")) != str(provided.get(field, "")):
            fields.append(field)
            reasons.append(f"{field}不一致")
    return fields, reasons


def _find_row(lineage_id: int | None) -> dict[str, Any] | None:
    if lineage_id is None:
        return None
    return store.find("lineage_graph", int(lineage_id))


def _next_lineage_id() -> int:
    rows = store.rows("lineage_graph")
    return max((int(row.get("id", 0) or 0) for row in rows), default=0) + 1


def _append_row(record: Mapping[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": _next_lineage_id(),
        "status": "已建立",
        "目标表": record.get("目标表", ""),
        "来源表列表": record.get("来源表列表", ""),
        "转换逻辑": record.get("转换逻辑", ""),
        "中间层级": record.get("中间层级", "1"),
        "血缘深度": record.get("血缘深度", "1"),
        "pending": False,
        "abnormal": False,
    }
    if "snapshot_id" in record:
        row["snapshot_id"] = record["snapshot_id"]
    store.rows("lineage_graph").append(row)
    return row


def _sync_snapshot_tag(record: Mapping[str, Any], lineage_id: int) -> None:
    """批次续写时补上快照编号，保证既有快照血缘仍可按编号定位。"""
    if "snapshot_id" not in record:
        return
    row = _find_row(lineage_id)
    if row is not None and row.get("snapshot_id") != record["snapshot_id"]:
        row["snapshot_id"] = record["snapshot_id"]


def _resolve_key(batch_id: object, source: str | None) -> str:
    """把调用方给的批次编号解析成命名空间内的键。

    没给来源时：唯一命中就用命中的来源；同号多来源视为歧义；没有登记则默认
    按目录同步命名空间返回，便于给出明确的“未找到”结果。
    """
    if source:
        return batch_key(source, batch_id)
    key = str(batch_id)
    if key in _batches:
        return key
    casefold = key.casefold()
    matches = [
        candidate for candidate in _batches
        if candidate.rsplit(":", 1)[-1].casefold() == casefold
    ]
    if len(matches) == 1:
        return matches[0]
    return batch_key(SOURCE_CATALOG, batch_id)


def _ambiguous_view(batch_id: object, matches: Sequence[str]) -> dict[str, Any]:
    return {
        "found": False,
        "ambiguous": True,
        "batch_id": str(batch_id),
        "matches": list(matches),
        "complete": False,
        "conflict": False,
        "conflicts": [],
        "lineages": [],
        "lineage_ids": [],
        "partial_lineages": [],
        "source_tables": [],
        "message": "该编号在多个来源下登记过，请指明来源",
    }


def _locate_slot(
    expected: Sequence[Mapping[str, Any]],
    provided: Mapping[str, Any],
    taken: set[int],
) -> int | None:
    """部分提交时，按相同批次标记 + 至少一侧对象相同来定位它原本属于哪一条。

    例如只提交改过的第三条关系时，仍能报告它是第 3 条（下标 2）冲突，
    而不是误判成第一条。
    """
    if not str(provided.get("转换逻辑", "")):
        return None
    candidates: list[int] = []
    for index, expected_record in enumerate(expected):
        if index in taken or not expected_record:
            continue
        if str(expected_record.get("转换逻辑", "")) != str(provided.get("转换逻辑", "")):
            continue
        same_target = str(expected_record.get("目标表", "")) == str(provided.get("目标表", ""))
        same_sources = _object_scope(expected_record)[1] == _object_scope(provided)[1]
        if same_target or same_sources:
            candidates.append(index)
    return candidates[0] if len(candidates) == 1 else None


def _position_relations(
    expected: Sequence[Mapping[str, Any]],
    relation_list: Sequence[Mapping[str, Any]],
    start_index: int,
    slots: Sequence[int | None],
) -> list[tuple[int, Mapping[str, Any]]]:
    """把本次提交的关系对齐到批次里的位置。

    全部提交时按顺序；只提交一部分时，优先按既有行/批次标记定位，
    这样“只提交改过的第三条”也能报出它原本属于第 3 条。
    """
    partial = len(relation_list) < len(expected)
    taken: set[int] = set()
    result: list[tuple[int, Mapping[str, Any]]] = []
    for offset, provided in enumerate(relation_list):
        index: int | None = None
        if partial:
            index = _locate_slot(expected, provided, taken)
            if index is None and start_index == 0 and len(relation_list) == 1:
                marker = str(provided.get("转换逻辑", ""))
                same_marker_unregistered = [
                    i for i, record in enumerate(expected)
                    if i not in taken and record
                    and str(record.get("转换逻辑", "")) == marker
                    and slots[i] is None
                ]
                if len(same_marker_unregistered) == 1:
                    index = same_marker_unregistered[0]
        if index is None:
            index = start_index + offset
        taken.add(index)
        result.append((index, provided))
    return result


def _view(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """输出批次状态；未齐全时把已有记录放在 partial_lineages，而不是 lineages。"""
    expected: list[dict[str, Any]] = list(manifest["expected"])
    slots: list[int | None] = list(manifest["lineage_ids"])
    entries: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    missing: list[int] = []
    for index, expected_record in enumerate(expected):
        lineage_id = slots[index] if index < len(slots) else None
        row = _find_row(lineage_id)
        registered = row is not None
        if not registered:
            missing.append(index)
        else:
            rows.append(row)
        entries.append({
            "index": index,
            "lineage_id": row.get("id") if row else lineage_id,
            "registered": registered,
            "目标表": expected_record.get("目标表", ""),
            "来源表列表": expected_record.get("来源表列表", ""),
            "转换逻辑": expected_record.get("转换逻辑", ""),
        })

    complete = not missing and len(rows) == len(expected)
    sources = [_split_sources(row.get("来源表列表", "")) for row in rows]
    return {
        "found": True,
        "batch_id": manifest["batch_id"],
        "source": manifest["source"],
        "owner_id": manifest["owner_id"],
        "expected_count": len(expected),
        "registered_count": len(rows),
        "complete": complete,
        "missing_indices": missing,
        "conflict": False,
        "conflicts": [],
        "entries": entries,
        # 组不齐全时 lineages 为空：调用方不能把已有的部分记录当成整组生效。
        "lineages": rows if complete else [],
        "lineage_ids": [row.get("id") for row in rows] if complete else [],
        "partial_lineages": [] if complete else rows,
        "source_tables": sources if complete else [],
    }


def _conflict_view(
    manifest: Mapping[str, Any],
    conflicts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    view = _view(manifest)
    view["ok"] = False
    view["found"] = True
    view["complete"] = False
    view["conflict"] = True
    view["conflicts"] = list(conflicts)
    view["conflict_indices"] = [int(item["index"]) for item in conflicts]
    view["conflicting_index"] = int(conflicts[0]["index"]) if conflicts else None
    view["lineages"] = []
    view["lineage_ids"] = []
    view["source_tables"] = []
    view["message"] = "提交的关系与批次最初登记的对象范围冲突，已登记血缘保持原样"
    return view


def _coerce_limit(options: Mapping[str, Any]) -> int | None:
    for alias in _INTERRUPT_OPTIONS:
        if alias not in options:
            continue
        value = options[alias]
        if value is None:
            continue
        if alias in ("only_first", "first_only"):
            if value:
                return 1
            continue
        return int(value)
    if options.get("interrupt"):
        return 1
    return None


def _coerce_expected_count(options: Mapping[str, Any]) -> int | None:
    for alias in ("expected_count", "total_count", "batch_size", "count_total"):
        if options.get(alias) is not None:
            return int(options[alias])
    return None


def _coerce_start_index(options: Mapping[str, Any]) -> int:
    for alias in ("start_index", "index", "batch_index", "offset"):
        if options.get(alias) is not None:
            return int(options[alias])
    return 0


def _register(
    records: Sequence[Mapping[str, Any]],
    *,
    batch_id: object,
    source: str = "",
    owner_id: object | None = None,
    expected_count: int | None = None,
    limit: int | None = None,
    start_index: int = 0,
    dry_run: bool = False,
) -> dict[str, Any]:
    """登记一批血缘关系（目录同步与快照生成共用）。"""
    if limit is not None and limit < 0:
        raise ValueError("limit 不能为负数")
    if start_index < 0:
        raise ValueError("start_index 不能为负数")

    relation_list = _normalize_records(records)
    if expected_count is not None:
        expected_count = int(expected_count)
        if expected_count < len(relation_list):
            expected_count = len(relation_list)
    if expected_count is None:
        # 目录同步固定登记三条来源关系；其他来源按本次提交的关系数推断。
        expected_count = CATALOG_LINEAGE_COUNT if source == SOURCE_CATALOG else (
            len(relation_list) + start_index)

    key = batch_key(source, batch_id) if source else str(batch_id)

    def _fresh_manifest(length: int) -> dict[str, Any]:
        expected: list[dict[str, Any]] = [{} for _ in range(max(length, 0))]
        for offset, provided in enumerate(relation_list):
            index = start_index + offset
            if index < len(expected):
                expected[index] = provided
        return {
            "batch_id": key,
            "source": source or str(key).split(":", 1)[0],
            "owner_id": owner_id if owner_id is not None else batch_id,
            "expected": expected,
            "lineage_ids": [None] * len(expected),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    with _lock:
        manifest = _batches.get(key)
        if manifest is None:
            length = max(len(relation_list) + start_index, expected_count or 0)
            manifest = _fresh_manifest(length)
            if dry_run:
                view = _preview_view(manifest, relation_list, start_index)
                view.update({"written_count": 0, "written_lineage_ids": [],
                             "reused_count": 0, "reused_lineage_ids": [],
                             "registered_lineage_ids": []})
                return view
            _batches[key] = manifest
        else:
            if source and manifest.get("source") not in ("", source):
                return _conflict_view(manifest, [{
                    "index": 0,
                    "reason": "批次来源不一致",
                    "expected_source": manifest.get("source"),
                    "provided_source": source,
                    "different_fields": ["source"],
                }])

        expected: list[dict[str, Any]] = manifest["expected"]
        slots: list[int | None] = manifest["lineage_ids"]

        positioned = _position_relations(
            expected, relation_list, start_index, slots)

        if dry_run:
            view = _preview_view(manifest, relation_list, start_index, positioned)
            view.update({"written_count": 0, "written_lineage_ids": [],
                         "reused_count": 0, "reused_lineage_ids": [],
                         "registered_lineage_ids": []})
            return view

        # 先整体校验，再决定是否写入；有冲突时任何记录都不改动。
        conflicts: list[dict[str, Any]] = []
        for index, provided in positioned:
            if index >= len(expected):
                conflicts.append({
                    "index": index,
                    "reason": "超出批次最初约定的对象范围",
                    "provided": dict(provided),
                    "different_fields": list(LINEAGE_FIELDS),
                })
                continue
            expected_record = expected[index]
            if not expected_record:
                # 首次登记时只声明了数量、没有声明内容的位置，后续提交视为补写。
                continue
            different_fields, reasons = _scope_diff(expected_record, provided)
            if different_fields:
                conflicts.append({
                    "index": index,
                    "reason": "；".join(reasons),
                    "conflict_type": "object_scope" if {"目标表", "来源表列表"} & set(different_fields)
                    else "relation_detail",
                    "expected": dict(expected_record),
                    "provided": dict(provided),
                    "expected_target": expected_record.get("目标表", ""),
                    "provided_target": provided.get("目标表", ""),
                    "expected_sources": _split_sources(expected_record.get("来源表列表", "")),
                    "provided_sources": _split_sources(provided.get("来源表列表", "")),
                    "different_fields": different_fields,
                })

        if conflicts:
            return _conflict_view(manifest, conflicts)

        # 校验通过后，未约定内容的位置用本次提交补齐。
        for index, provided in positioned:
            if index < len(expected) and not expected[index]:
                expected[index] = provided
        written: list[int] = []
        reused: list[int] = []
        ordered: list[int] = []
        batch_size = len(expected)
        for index, expected_record in enumerate(expected):
            if not expected_record:
                continue
            previous = slots[index] if index < len(slots) else None
            existing_row = _find_row(previous)
            if existing_row is not None:
                _sync_snapshot_tag(expected_record, int(existing_row["id"]))
                reused.append(int(existing_row.get("id")))
                ordered.append(int(existing_row.get("id")))
                continue
            if limit is not None and len(written) >= limit:
                continue
            row = _append_row(expected_record)
            slots[index] = int(row["id"])
            _row_meta[int(row["id"])] = {
                "batch_id": key,
                "batch_index": index,
                "batch_size": batch_size,
                "batch_source": manifest["source"],
                "owner_id": manifest["owner_id"],
                "expected": dict(expected_record),
            }
            written.append(int(row["id"]))
            ordered.append(int(row["id"]))

        view = _view(manifest)
        view["ok"] = True
        view["written_count"] = len(written)
        view["written_lineage_ids"] = written
        view["reused_count"] = len(reused)
        view["reused_lineage_ids"] = reused
        view["registered_lineage_ids"] = ordered
        view["partial"] = not view["complete"]
        view["interrupted"] = limit is not None and not view["complete"]
        view["message"] = (
            f"批次 {key} 已齐全，共 {view['registered_count']} 条血缘关系"
            if view["complete"]
            else f"批次 {key} 已登记 {view['registered_count']}/{view['expected_count']} 条，可继续补写"
        )
        return view


def _preview_view(
    manifest: Mapping[str, Any],
    relation_list: Sequence[Mapping[str, Any]],
    start_index: int,
    positioned: Sequence[tuple[int, Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """试运行视图：只报告位置和差异，不写入任何记录。"""
    expected: list[dict[str, Any]] = list(manifest["expected"])
    conflicts: list[dict[str, Any]] = []
    pairs = positioned if positioned is not None else [
        (start_index + offset, provided) for offset, provided in enumerate(relation_list)
    ]
    for index, provided in pairs:
        if index >= len(expected):
            conflicts.append({
                "index": index,
                "reason": "超出批次最初约定的对象范围",
                "provided": dict(provided),
                "different_fields": list(LINEAGE_FIELDS),
            })
            continue
        expected_record = expected[index]
        if not expected_record:
            continue
        different_fields, reasons = _scope_diff(expected_record, provided)
        if different_fields:
            conflicts.append({
                "index": index,
                "reason": "；".join(reasons),
                "conflict_type": "object_scope"
                if {"目标表", "来源表列表"} & set(different_fields) else "relation_detail",
                "expected": dict(expected_record),
                "provided": dict(provided),
                "different_fields": different_fields,
            })
    if conflicts:
        view = _conflict_view(manifest, conflicts)
    else:
        view = _view(manifest)
        view["ok"] = True
        view["conflict"] = False
        view["dry_run"] = True
    view["dry_run"] = True
    return view


def submit_lineage(*args: Any, **options: Any) -> dict[str, Any]:
    """预检一批关系是否与批次最初的对象范围冲突；默认不写入任何记录。

    传入 ``apply=True`` 时可一次完成预检与写入，冲突时同样不写入。
    """
    apply_changes = bool(options.pop("apply", False))
    options["dry_run"] = not apply_changes
    return register_lineage(*args, **options)


check_lineage = submit_lineage
validate_lineage = submit_lineage
preview_lineage = submit_lineage


def _looks_like_records(value: object) -> bool:
    if isinstance(value, Mapping):
        return True
    return isinstance(value, (list, tuple)) and (
        not value or not isinstance(value[0], str)
    )


def register_lineage(*args: Any, **options: Any) -> dict[str, Any]:
    """登记一批血缘关系；兼容 ``(records, batch_id=...)`` 与 ``(batch_id, records)``。"""
    records: Sequence[Mapping[str, Any]] | None = options.pop("records", None)
    lines: Sequence[Mapping[str, Any]] | None = options.pop("relationships", None)
    lines = lines if lines is not None else options.pop("relations", None)
    records = records if records is not None else lines

    batch_id = options.pop("batch_id", None)
    owner_id = options.pop("owner_id", None)
    if batch_id is None:
        batch_id = options.pop("id", None)
    if batch_id is None:
        batch_id = owner_id
    source = str(options.pop("source", "") or "")

    positional = list(args)
    owner_from_positional: object | None = None
    if records is None:
        if len(positional) >= 2 and not _looks_like_records(positional[0]) and _looks_like_records(positional[1]):
            owner_from_positional = positional[0]
            records = positional[1]
        elif positional and _looks_like_records(positional[0]):
            records = positional[0]
            if len(positional) >= 2 and not isinstance(positional[1], Mapping):
                owner_from_positional = positional[1]
        elif positional:
            owner_from_positional = positional[0]
    if records is None:
        records = []
    if batch_id is None:
        batch_id = owner_from_positional
    if batch_id is None:
        # 最后再尝试从剩余关键字里取一次，保证错误信息可读。
        batch_id = options.pop("batch", None)
    if batch_id is None:
        raise ValueError("登记血缘必须提供 batch_id")

    limit = _coerce_limit(options)
    expected_count = _coerce_expected_count(options)
    start_index = _coerce_start_index(options)
    dry_run = bool(options.pop("dry_run", False)) or bool(options.pop("preview", False))
    if source == "" and ":" in str(batch_id):
        head, _, tail = str(batch_id).partition(":")
        source, batch_id = head, tail
    if owner_id is None:
        owner_id = batch_id

    return _register(
        records,
        batch_id=batch_id,
        source=source,
        owner_id=owner_id,
        expected_count=expected_count,
        limit=limit,
        start_index=start_index,
        dry_run=dry_run,
    )


register = register_lineage
register_batch = register_lineage
register_lineage_batch = register_lineage
register_batch_lineage = register_lineage
record_lineage = register_lineage
write_lineage = register_lineage
submit_batch = register_lineage


def batch_status(batch_id: object, source: str | None = None) -> dict[str, Any]:
    """按批次编号回查一组血缘关系；未齐全时 complete=False 且 lineages 为空。"""
    with _lock:
        key = str(batch_id)
        if not source and key not in _batches:
            # 未指明来源时，同一编号在多个来源下登记过就不能猜。
            suffix = f":{key}"
            matches = [candidate for candidate in _batches if candidate.endswith(suffix)]
            if len(matches) > 1:
                return _ambiguous_view(batch_id, matches)
        key = _resolve_key(batch_id, source)
        manifest = _batches.get(key)
        if manifest is None:
            return {
                "found": False,
                "batch_id": key,
                "source": source or "",
                "complete": False,
                "conflict": False,
                "conflicts": [],
                "lineages": [],
                "lineage_ids": [],
                "partial_lineages": [],
                "source_tables": [],
                "message": "未找到该批次的血缘登记",
            }
        return _view(manifest)


get_batch = batch_status
lookup_batch = batch_status
query_batch = batch_status
get_batch_lineage = batch_status
lookup_batch_lineage = batch_status
query_lineage_batch = batch_status


def snapshot_lineage(
    snapshot_id: object,
    *,
    source: str = SOURCE_SNAPSHOT,
) -> dict[str, Any]:
    """按快照任务编号定位快照自己登记的那一条来源关系。"""
    with _lock:
        key = batch_key(source, snapshot_id)
        manifest = _batches.get(key)
        if manifest is None:
            # 兼容直接携带 snapshot_id 的既有记录：只认快照来源，不串用目录批次。
            for row in store.rows("lineage_graph"):
                if str(row.get("snapshot_id", "")) == str(snapshot_id):
                    return _snapshot_view(snapshot_id, row)
            return _snapshot_missing(snapshot_id, key)

        partial = _view(manifest)
        row: dict[str, Any] | None = None
        if partial["complete"] and partial["lineages"]:
            row = partial["lineages"][0]
        elif partial.get("partial_lineages"):
            row = partial["partial_lineages"][0]
        if row is None:
            return _snapshot_missing(snapshot_id, key)

        result = _snapshot_view(snapshot_id, row)
        result.update({
            "batch_id": key,
            "complete": partial["complete"],
            "lineage_ids": partial["lineage_ids"],
        })
        return result


def _snapshot_missing(snapshot_id: object, key: str) -> dict[str, Any]:
    return {
        "found": False,
        "snapshot_id": snapshot_id,
        "batch_id": key,
        "complete": False,
        "lineage": None,
        "lineage_id": None,
        "source_tables": [],
        "sources": [],
        "message": "该快照尚未登记血缘",
    }


def _snapshot_view(snapshot_id: object, row: Mapping[str, Any]) -> dict[str, Any]:
    sources = _split_sources(row.get("来源表列表", ""))
    return {
        "found": True,
        "snapshot_id": snapshot_id,
        "lineage_id": row.get("id"),
        "lineage": dict(row),
        "target_table": row.get("目标表", ""),
        "来源表列表": row.get("来源表列表", ""),
        "source_tables": sources,
        "sources": sources,
        "complete": True,
    }


get_snapshot_lineage = snapshot_lineage
lookup_snapshot = snapshot_lineage
lookup_snapshot_lineage = snapshot_lineage
query_snapshot = snapshot_lineage


def lineage_metadata(lineage_id: int) -> dict[str, Any] | None:
    """返回某条血缘的登记元数据（批次、序号、来源模块），供内部与回查使用。"""
    with _lock:
        meta = _row_meta.get(int(lineage_id))
        return dict(meta) if meta else None


def reset() -> None:
    """清空登记器内存状态（测试/热重载用，不影响已写入的 lineage_graph 行）。"""
    with _lock:
        _batches.clear()
        _row_meta.clear()


class LineageRegistry:
    """面向对象的薄封装：目录同步与快照生成共用同一个登记器实现。"""

    SOURCE_CATALOG = SOURCE_CATALOG
    SOURCE_SNAPSHOT = SOURCE_SNAPSHOT
    FIELDS = LINEAGE_FIELDS

    @staticmethod
    def batch_key(source: str, owner_id: object) -> str:
        return batch_key(source, owner_id)

    @staticmethod
    def register(records: Any = None, **options: Any) -> dict[str, Any]:
        if records is None:
            return register_lineage(**options)
        return register_lineage(records, **options)

    register_batch = register
    register_lineage = register

    @staticmethod
    def status(batch_id: object, source: str | None = None) -> dict[str, Any]:
        return batch_status(batch_id, source=source)

    get_batch = status
    batch_status = status
    lookup = status

    @staticmethod
    def snapshot(snapshot_id: object) -> dict[str, Any]:
        return snapshot_lineage(snapshot_id)

    snapshot_lineage = snapshot
    lookup_snapshot = snapshot

    @staticmethod
    def metadata(lineage_id: int) -> dict[str, Any] | None:
        return lineage_metadata(lineage_id)

    @staticmethod
    def reset() -> None:
        reset()


registry = LineageRegistry()

