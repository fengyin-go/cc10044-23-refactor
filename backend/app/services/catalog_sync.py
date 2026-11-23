"""目录同步领域服务：同步不同Catalog或Metastore之间的元数据和表结构。

数据链路位置：元数据同步层，保持多个数据目录的结构一致性。"""

from __future__ import annotations

from typing import Any, Sequence

from app.services import lineage_registry
from app.services.base import BaseModuleService
from app.store import store


class CatalogSyncService(BaseModuleService):
    MODULE = "catalog_sync"
    ENTITY = "同步任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务名称", "源Catalog", "目标Catalog", "同步范围", "同步状态"]
    REQUIRED_FIELDS = ["任务名称", "源Catalog", "目标Catalog"]
    STATUS_ORDER = ["待同步", "同步中", "已同步", "同步失败"]
    ACTION_RULES = {"启动同步": "待同步", "取消同步": "同步中"}
    NEGATIVE_ACTIONS = []

    def start_sync(self, entry_id: int, incremental: bool = True) -> dict[str, Any]:
        """启动目录同步任务。

        incremental=True 时只同步上次同步以来发生变更的表，
        incremental=False 时全量重新同步。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "同步任务不存在"}
        if entry.get("status") != "待同步":
            return {"ok": False, "message": "只能启动待同步状态的同步任务"}

        source_catalog = entry.get("源Catalog", "")
        target_catalog = entry.get("目标Catalog", "")
        sync_scope = entry.get("同步范围", "")

        entry["status"] = "同步中"
        tables_to_sync = self._resolve_sync_tables(source_catalog, sync_scope)
        return {
            "ok": True,
            "task_id": entry_id,
            "source_catalog": source_catalog,
            "target_catalog": target_catalog,
            "incremental": incremental,
            "tables_to_sync": len(tables_to_sync),
            "estimated_duration_minutes": 12,
            "tables": tables_to_sync[:20],
        }

    def validate_sync_completeness(self, entry_id: int) -> dict[str, Any]:
        """验证同步完整性。

        对比源和目标Catalog中的表列表、Schema和分区定义，
        检测遗漏、不一致或版本差异。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "同步任务不存在"}

        checks = [
            {"check": "table_list_match", "source_count": 320, "target_count": 320, "matched": True, "mismatched_tables": []},
            {"check": "schema_match", "tables_with_diff": 2, "diff_details": [
                {"table": "order_fact_v3", "source_columns": 48, "target_columns": 47,
                 "missing_in_target": ["promotion_code"]},
                {"table": "user_behavior_log", "source_columns": 22, "target_columns": 22,
                 "type_diff": ["event_timestamp: TIMESTAMP vs STRING"]},
            ], "matched": False},
            {"check": "partition_match", "total_tables": 320, "mismatched": 0, "matched": True},
            {"check": "table_properties_match", "mismatched": 1, "details": [
                {"table": "dashboard_cache", "prop": "format-version", "source": "2", "target": "1"}
            ], "matched": False},
        ]
        all_ok = all(c["matched"] for c in checks)
        if all_ok:
            entry["status"] = "已同步"
            entry["同步状态"] = "已完成"
            entry["pending"] = False
        else:
            entry["abnormal"] = True

        return {
            "ok": all_ok,
            "task_id": entry_id,
            "checks": checks,
            "verdict": "同步完整" if all_ok else "存在差异需要修复",
        }

    def detect_schema_conflicts(self, entry_id: int) -> dict[str, Any]:
        """检测目录同步中的Schema冲突。

        当源和目标Catalog对同一表的Schema定义不一致时，
        列出冲突详情并提供解决建议。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "同步任务不存在"}

        return {
            "task_id": entry_id,
            "conflict_count": 3,
            "conflicts": [
                {
                    "table": "user_events",
                    "conflict_type": "column_type_mismatch",
                    "source": "event_data: MAP<STRING,STRING>",
                    "target": "event_data: STRING",
                    "resolution": "建议以源端类型为准，目标端已过期",
                },
                {
                    "table": "dim_customer",
                    "conflict_type": "missing_column",
                    "column": "loyalty_tier",
                    "resolution": "在目标端添加该列，默认值设为'standard'",
                },
                {
                    "table": "sales_summary",
                    "conflict_type": "partition_diff",
                    "source_partition": "dt",
                    "target_partition": "date_str",
                    "resolution": "按源端分区键重新创建目标表",
                },
            ],
            "auto_resolvable": 2,
            "manual_intervention_required": 1,
        }

    def sync_with_lineage(self, entry_id: int, **options: Any) -> dict[str, Any]:
        """同步完成后更新血缘关系。

        将同步范围内的表在目标Catalog的位置注册到血缘图谱，
        建立源→目标的跨Catalog血缘链路。

        登记走共用血缘登记器，与快照生成器共用写入过程；返回字段保持原样。
        额外关键字（如 ``limit=1``）可让本次只写入部分关系，模拟中断后用同一
        任务编号再次调用即可把同批办完。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "同步任务不存在"}

        options.pop("source", None)
        options.pop("owner_id", None)
        result = lineage_registry.register_lineage(
            self._catalog_lineage_records(entry_id),
            batch_id=entry_id,
            source=lineage_registry.SOURCE_CATALOG,
            owner_id=entry_id,
            **options,
        )
        registered_ids = list(result.get("registered_lineage_ids", []))

        return {
            "ok": True,
            "task_id": entry_id,
            "new_lineage_ids": registered_ids,
            "message": f"已注册 {len(registered_ids)} 条血缘关系",
        }

    def _catalog_lineage_records(self, entry_id: int) -> list[dict[str, Any]]:
        """构造目录同步要登记的三条来源关系（字段格式与 lineage_graph 一致）。"""
        entry = store.find(self.MODULE, entry_id) or {}
        target_catalog = entry.get("目标Catalog")
        source_catalog = entry.get("源Catalog")
        scope = entry.get("同步范围", "table")
        return [
            {
                "目标表": f"{target_catalog}_{scope}_{i}",
                "来源表列表": f"{source_catalog}_{scope}_{i}",
                "转换逻辑": f"catalog_sync_{entry_id}",
                "中间层级": "1",
                "血缘深度": "1",
            }
            for i in range(3)
        ]

    def resume_lineage_registration(self, entry_id: int, **options: Any) -> dict[str, Any]:
        """把上次中断的目录同步血缘批次继续办完，并回报整组状态。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "同步任务不存在"}

        batch_id = options.get("batch_id", entry_id)
        lineage_registry.register_lineage(
            self._catalog_lineage_records(entry_id),
            batch_id=batch_id,
            source=lineage_registry.SOURCE_CATALOG,
            owner_id=entry_id,
        )
        status = lineage_registry.batch_status(
            batch_id, source=lineage_registry.SOURCE_CATALOG)
        return {
            "ok": status["complete"],
            "task_id": entry_id,
            "batch_id": status["batch_id"],
            "complete": status["complete"],
            "registered_count": status["registered_count"],
            "expected_count": status["expected_count"],
            "lineage_ids": status["lineage_ids"],
            "registered_lineages": status["lineages"],
        }

    def query_registered_lineage(self, entry_id: int, **options: Any) -> dict[str, Any]:
        """回查本任务登记的血缘批次；组不齐全时不把已有记录当作整组生效。

        可传 ``batch_id`` 指定其他批次，或 ``source`` 指定来源命名空间。
        """
        batch_id = options.get("batch_id", entry_id)
        source = options.get("source") or lineage_registry.SOURCE_CATALOG
        status = lineage_registry.batch_status(batch_id, source=source)
        return {
            "ok": status["complete"],
            "task_id": entry_id,
            **status,
        }

    def register_lineage_batch(
        self,
        entry_id: int,
        records: Sequence[dict[str, Any]] | None = None,
        *,
        batch_id: object | None = None,
        expected_count: int | None = None,
        limit: int | None = None,
        start_index: int = 0,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """用指定批次编号登记/补写目录同步血缘。

        ``records`` 省略时使用该任务默认的三条来源关系；``limit=1`` 表示只写入
        第一条后中断，后续用同一 ``entry_id`` 再次调用即可把同批办完；
        ``start_index`` 可只提交批次中剩余的关系；``dry_run`` 只预检冲突不写入。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "同步任务不存在"}
        relations = list(records) if records is not None else self._catalog_lineage_records(entry_id)
        return lineage_registry.register_lineage(
            relations,
            batch_id=batch_id if batch_id is not None else entry_id,
            source=lineage_registry.SOURCE_CATALOG,
            owner_id=entry_id,
            expected_count=expected_count,
            limit=limit,
            start_index=start_index,
            dry_run=dry_run,
        )

    def _resolve_sync_tables(self, catalog: str, scope: str) -> list[str]:
        base_tables = [
            "order_fact", "user_dim", "product_dim", "sales_agg_daily",
            "inventory_snapshot", "marketing_campaign", "web_traffic_log",
            "customer_360", "supplier_master", "finance_ledger",
        ]
        if scope and scope != "全部":
            return [t for t in base_tables if scope in t]
        return base_tables