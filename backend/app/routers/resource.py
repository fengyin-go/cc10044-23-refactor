"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.get("/{module}/{entry_id}/lineage", response_model=dict)
def query_lineage(module: str, entry_id: int, request: Request) -> dict[str, Any]:
    """回查某个业务对象登记的血缘。

    * 目录同步：按批次回查整组，返回 complete 标记；组不齐全时 lineages 为空；
    * 快照生成器：按快照编号定位它自己登记的那一条来源关系。

    可选查询参数 ``batch_id``、``source`` 用于显式指定批次编号或来源命名空间。
    """
    service = require_service(module)
    options = {
        key: value for key, value in request.query_params.items()
        if key in ("batch_id", "source") and value.strip()
    }
    if hasattr(service, "query_registered_lineage"):
        return service.query_registered_lineage(entry_id, **options)
    if hasattr(service, "query_snapshot_lineage"):
        return service.query_snapshot_lineage(entry_id)
    raise HTTPException(status_code=404, detail=f"{module} 未登记血缘")


@router.post("/{module}/{entry_id}/lineage", response_model=dict)
def register_lineage(
    module: str,
    entry_id: int,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """登记/续写某个业务对象的血缘批次。

    请求体可选：``records`` 指定关系，``expected_count`` 指定整组条数，
    ``limit`` 只写入前几条（模拟中断），``start_index`` 指定从第几条续写，
    ``dry_run`` 只预检冲突不写入。
    """
    service = require_service(module)
    body = payload or {}
    if hasattr(service, "register_lineage_batch"):
        known = {"expected_count", "limit", "start_index", "dry_run", "batch_id"}
        extra = {key: value for key, value in body.items() if key in known}
        records = body.get("records")
        if records is not None and not isinstance(records, (list, dict)):
            raise HTTPException(status_code=422, detail="records 必须是关系列表")
        return service.register_lineage_batch(entry_id, records, **extra)
    if hasattr(service, "query_snapshot_lineage"):
        return service.create_snapshot_with_lineage(entry_id)
    raise HTTPException(status_code=404, detail=f"{module} 未登记血缘")
