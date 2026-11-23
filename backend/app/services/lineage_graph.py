"""数据血缘领域服务：管理表级和字段级的数据血缘图、上下游追踪和断裂检测。

数据链路位置：元数据层，维护数据从源到目标的完整血缘关系图。"""

from __future__ import annotations

from typing import Any

from app.services import lineage_registry
from app.services.base import BaseModuleService
from app.store import store


class LineageGraphService(BaseModuleService):
    MODULE = "lineage_graph"
    ENTITY = "血缘关系"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["目标表", "来源表列表", "转换逻辑", "中间层级", "血缘深度"]
    REQUIRED_FIELDS = ["目标表", "来源表列表", "转换逻辑"]
    STATUS_ORDER = ["已建立", "待验证", "已断裂"]
    ACTION_RULES = {"刷新血缘": "已建立", "标记断裂": "已断裂"}
    NEGATIVE_ACTIONS = []

    def trace_upstream(self, target_table: str, max_depth: int = 5) -> dict[str, Any]:
        """从目标表向上追溯所有上游来源表和转换链路。

        返回完整的上游依赖树，每个节点包含表名、所在层级和转换逻辑。
        用于变更影响分析。
        """
        all_entries = store.rows(self.MODULE)
        visited: set[str] = set()
        tree: list[dict[str, Any]] = []
        queue = [(target_table, 0)]
        while queue and len(visited) < 50:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for entry in all_entries:
                if entry.get("目标表") == current and entry.get("status") == "已建立":
                    sources = entry.get("来源表列表", "").split(",")
                    node = {
                        "table": current,
                        "depth": depth,
                        "sources": [s.strip() for s in sources if s.strip()],
                        "transform": entry.get("转换逻辑"),
                        "lineage_id": entry.get("id"),
                    }
                    tree.append(node)
                    for src in node["sources"]:
                        if src not in visited:
                            queue.append((src, depth + 1))

        return {
            "target_table": target_table,
            "max_depth": max_depth,
            "total_nodes": len(tree),
            "upstream_tree": tree,
            "leaf_tables": [n["table"] for n in tree if not n["sources"]],
        }

    def trace_downstream(self, source_table: str, max_depth: int = 5) -> dict[str, Any]:
        """从来源表向下追溯所有下游受影响表和转换链路。

        用于评估某个表变更时的影响范围，联动质量规则模块通知下游。
        """
        all_entries = store.rows(self.MODULE)
        visited: set[str] = set()
        tree: list[dict[str, Any]] = []
        queue = [(source_table, 0)]
        while queue and len(visited) < 50:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for entry in all_entries:
                sources = entry.get("来源表列表", "").split(",")
                source_set = {s.strip() for s in sources if s.strip()}
                if current in source_set and entry.get("status") == "已建立":
                    target = entry.get("目标表", "")
                    node = {
                        "table": target,
                        "depth": depth + 1,
                        "upstream_of": current,
                        "transform": entry.get("转换逻辑"),
                        "lineage_id": entry.get("id"),
                    }
                    tree.append(node)
                    if target not in visited:
                        queue.append((target, depth + 1))

        return {
            "source_table": source_table,
            "max_depth": max_depth,
            "total_affected": len(tree),
            "downstream_tree": tree,
            "end_consumers": [n["table"] for n in tree if not any(
                e.get("来源表列表", "").find(n["table"]) >= 0
                for e in all_entries if e.get("status") == "已建立"
            )],
        }

    def detect_broken_lineage(self) -> dict[str, Any]:
        """扫描并检测断裂的血缘关系。

        当上游来源表已被迁移、归档或删除时，其下游血缘关系变为断裂状态。
        检测结果包含受影响的目标表和对应的修复建议。
        """
        all_entries = store.rows(self.MODULE)
        broken: list[dict[str, Any]] = []
        for entry in all_entries:
            if entry.get("status") == "已断裂":
                broken.append({
                    "lineage_id": entry.get("id"),
                    "target_table": entry.get("目标表"),
                    "sources": entry.get("来源表列表"),
                    "needs_revalidation": True,
                })

        table_archive_rows = store.rows("table_archive")
        migration_rows = store.rows("data_migration")

        for entry in all_entries:
            if entry.get("status") != "已建立":
                continue
            sources = entry.get("来源表列表", "").split(",")
            for src in sources:
                src = src.strip()
                if not src:
                    continue
                archived = any(
                    a.get("目标表") == src and a.get("status") == "已启用"
                    for a in table_archive_rows
                )
                if archived:
                    entry["status"] = "已断裂"
                    broken.append({
                        "lineage_id": entry.get("id"),
                        "target_table": entry.get("目标表"),
                        "source": src,
                        "reason": "来源表已归档",
                        "needs_revalidation": True,
                    })

        return {
            "broken_count": len(broken),
            "broken_lineages": broken,
            "suggestion": "请重新校验断裂血缘并更新目标表关联",
        }

    def build_inheritance_chain(self, table_name: str) -> dict[str, Any]:
        """为指定表构建完整的继承链（上下游合起来）。

        从该表出发，同时向上追溯祖先表和向下追溯后代表，
        生成完整的数据流转全景图。
        """
        upstream = self.trace_upstream(table_name)
        downstream = self.trace_downstream(table_name)
        return {
            "table": table_name,
            "upstream_depth": upstream["total_nodes"],
            "downstream_affected": downstream["total_affected"],
            "upstream_chain": upstream["upstream_tree"],
            "downstream_chain": downstream["downstream_tree"],
            "orphan_risk": upstream["total_nodes"] == 0 and downstream["total_affected"] > 0,
        }

    def lookup_registered_batch(
        self,
        batch_id: object,
        source: str | None = None,
    ) -> dict[str, Any]:
        """回查共用血缘登记器写入的批次记录。

        目录同步与快照生成都走同一个登记器，这里让血缘模块也能直接看到
        批次组是否齐全；齐全才返回 lineages。
        """
        return lineage_registry.batch_status(batch_id, source=source)

    def lookup_snapshot_lineage(self, snapshot_id: object) -> dict[str, Any]:
        """按快照任务编号回查快照自己登记的那一条来源关系。"""
        return lineage_registry.snapshot_lineage(snapshot_id)

    def impact_analysis(self, entry_id: int) -> dict[str, Any]:
        """变更影响分析：当某个血缘条目的转换逻辑发生变更时，
        评估对下游的级联影响范围和受影响的数据质量规则。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "血缘条目不存在"}

        target_table = entry.get("目标表", "")
        downstream = self.trace_downstream(target_table)

        quality_rows = store.rows("data_quality_rule")
        affected_quality_rules = [
            q for q in quality_rows
            if q.get("目标表") in [n["table"] for n in downstream["downstream_tree"]]
        ]

        return {
            "lineage_id": entry_id,
            "source_table": target_table,
            "change_type": "transform_logic_changed",
            "direct_downstream_tables": len(downstream["downstream_tree"]),
            "cascaded_tables": downstream["total_affected"],
            "affected_quality_rule_count": len(affected_quality_rules),
            "affected_quality_rules": [
                {"rule_id": r["id"], "rule_name": r.get("规则名称"), "target_table": r.get("目标表")}
                for r in affected_quality_rules[:10]
            ],
            "recommendation": "建议在变更后立即执行受影响的质量规则校验",
        }