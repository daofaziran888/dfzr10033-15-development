"""湖仓批量登记清单领域服务。

把同一数据集的多条湖仓路径作为一整组提交：
- 利用路径扫描发现的文件清单与字段清单批量接入湖仓数据；
- 同一路径的可重试版本（扫描版本号）可辨识，已确认文件只登记一次；
- 成功条目必须带完整扫描证据（Schema 字段 + 文件覆盖范围）；
- 扫描中断后的重试沿用同一批次编号，绝不冒充新批；
- 任一分区读不到时该路径保持等待重试，不能算作已接入，整组也不能提前置为可用；
- 数据源注册按本清单实际创建/更新可供联邦查询的来源，并把逐条结果回写到扫描回执清单。
"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

BATCH_MODULE = "lake_batch_register"
RECEIPT_MODULE = "lake_scan_receipt"

# 批次/回执状态
STATUS_DRAFT = "待提交"
STATUS_SCANNING = "扫描中"
STATUS_WAITING = "等待重试"
STATUS_AVAILABLE = "整组可用"
RECEIPT_PENDING = "待扫描"
RECEIPT_WAITING = "等待重试"
RECEIPT_SUCCESS = "已接入"


class LakeBatchRegisterService(BaseModuleService):
    MODULE = BATCH_MODULE
    ENTITY = "湖仓批量登记清单"
    OPERATION_TERM = "批量登记"
    LIST_FIELDS = [
        "清单编号", "数据集", "来源路径", "条目总数", "已接入数",
        "待重试数", "重试次数", "批次状态",
    ]
    REQUIRED_FIELDS = ["清单编号", "数据集", "来源路径"]
    STATUS_ORDER = [STATUS_DRAFT, STATUS_SCANNING, STATUS_WAITING, STATUS_AVAILABLE]
    ACTION_RULES = {
        "提交整组": STATUS_SCANNING,
        "重试整组": STATUS_WAITING,
        "确认整组可用": STATUS_AVAILABLE,
    }
    NEGATIVE_ACTIONS: list[str] = []

    # 故障注入：path_id -> 读不到的分区列表，模拟分区级扫描中断，便于测试恢复流程。
    def __init__(self) -> None:
        self._blocked_partitions: dict[int, list[str]] = {}

    def block_partition(self, path_id: int, partitions: list[str]) -> None:
        """标记某条路径当前读不到的分区（模拟扫描中断）。"""
        self._blocked_partitions[path_id] = list(partitions)

    def restore_partition(self, path_id: int, partitions: list[str]) -> None:
        """恢复某条路径的分区（模拟运维修复后重新可读）。"""
        blocked = [p for p in self._blocked_partitions.get(path_id, []) if p not in partitions]
        if blocked:
            self._blocked_partitions[path_id] = blocked
        else:
            self._blocked_partitions.pop(path_id, None)

    # ------------------------------------------------------------------
    # 清单创建
    # ------------------------------------------------------------------
    def create_manifest(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """创建批量登记清单，并按来源路径预生成扫描回执（每条路径一行）。"""
        storage_paths = _split_paths(values.get("来源路径", ""))
        missing = [
            field for field in ("清单编号", "数据集")
            if not str(values.get(field) or "").strip()
        ]
        if not storage_paths:
            missing.append("来源路径")
        if missing:
            return None, f"缺少必填字段：{'、'.join(dict.fromkeys(missing))}"

        dataset = str(values.get("数据集", "")).strip()
        manifest_no = str(values.get("清单编号", "")).strip()
        if any(row.get("清单编号") == manifest_no for row in store.rows(self.MODULE)):
            return None, f"批量登记清单编号{manifest_no}已存在，重试请提交原清单而非新建批次"
        path_rows = _resolve_path_rows(storage_paths)
        found_locations = {row.get("存储位置", "") for row in path_rows}
        unresolved = [p for p in storage_paths if p not in found_locations]
        if unresolved:
            return None, f"湖仓路径未登记，无法批量扫描：{'、'.join(unresolved)}"

        entry, create_missing = self.create_entry({
            "清单编号": values.get("清单编号"),
            "数据集": dataset,
            "来源路径": ", ".join(storage_paths),
        })
        if entry is None:
            return None, f"创建清单失败：{'、'.join(create_missing)}"

        entry["条目总数"] = str(len(path_rows))
        entry["已接入数"] = "0"
        entry["待重试数"] = str(len(path_rows))
        entry["重试次数"] = "0"
        entry["批次状态"] = STATUS_DRAFT
        entry["status"] = STATUS_DRAFT
        entry["dataset"] = dataset
        entry["path_ids"] = [int(row["id"]) for row in path_rows]

        # 预生成回执：每条路径一行，重试时按 (清单, 路径) 复用，绝不新增副本。
        for row in path_rows:
            self._upsert_receipt(
                manifest=entry,
                path_row=row,
                scan_evidence=None,
                status=RECEIPT_PENDING,
                message="清单已创建，等待整组扫描",
            )

        return entry, f"批量登记清单{entry['清单编号']}已创建，共{len(path_rows)}条路径"

    # ------------------------------------------------------------------
    # 整组提交 / 重试（同一批次编号贯穿始终）
    # ------------------------------------------------------------------
    def submit_manifest(
        self,
        manifest_id: int,
        *,
        is_retry: bool = False,
        blocked_partitions: dict[int, list[str]] | None = None,
    ) -> dict[str, Any]:
        """提交整组扫描并登记。重试必须传入同一清单 id，沿用原批次编号。"""
        manifest = store.find(self.MODULE, manifest_id)
        if manifest is None:
            return {"ok": False, "message": "批量登记清单不存在"}

        if blocked_partitions:
            for path_id, parts in blocked_partitions.items():
                self.block_partition(int(path_id), list(parts))

        if manifest["status"] == STATUS_AVAILABLE:
            return {"ok": False, "message": "整组已可用，无需重复提交", "manifest_id": manifest_id}

        if is_retry:
            manifest["重试次数"] = str(int(manifest.get("重试次数", 0)) + 1)
        manifest["批次状态"] = STATUS_SCANNING
        manifest["status"] = STATUS_SCANNING

        item_results: list[dict[str, Any]] = []
        for path_id in list(manifest.get("path_ids", [])):
            path_row = store.find("data_lake_read", path_id)
            if path_row is None:
                item_results.append({"ok": False, "path_id": path_id, "message": "湖仓路径不存在"})
                continue
            item_results.append(self._process_path(manifest, path_row))

        success = [r for r in item_results if r["ok"]]
        waiting = [r for r in item_results if not r["ok"]]
        manifest["已接入数"] = str(len(success))
        manifest["待重试数"] = str(len(waiting))
        all_available = len(waiting) == 0 and len(success) == len(item_results) and len(item_results) > 0
        if all_available:
            manifest["批次状态"] = STATUS_AVAILABLE
            manifest["status"] = STATUS_AVAILABLE
            manifest["pending"] = False
            manifest["abnormal"] = False
        else:
            # 分区读不到时整组保持等待重试，绝不能提前算作整组可用。
            manifest["批次状态"] = STATUS_WAITING
            manifest["status"] = STATUS_WAITING
            manifest["pending"] = True
            manifest["abnormal"] = True

        self._write_batch_audit(manifest, item_results, is_retry)

        return {
            "ok": True,
            "manifest_id": manifest_id,
            "manifest_no": manifest["清单编号"],
            "attempt": int(manifest.get("重试次数", 0)) + 1,
            "is_retry": is_retry,
            "total": len(item_results),
            "registered": len(success),
            "waiting_retry": len(waiting),
            "all_available": all_available,
            "items": item_results,
        }

    def retry_manifest(
        self,
        manifest_id: int,
        blocked_partitions: dict[int, list[str]] | None = None,
    ) -> dict[str, Any]:
        """扫描中断后的整组重试：同一批次编号，只补登失败路径，原成功条目不产生副本。"""
        return self.submit_manifest(
            manifest_id, is_retry=True, blocked_partitions=blocked_partitions
        )

    # ------------------------------------------------------------------
    # 单条路径处理
    # ------------------------------------------------------------------
    def _process_path(self, manifest: dict[str, Any], path_row: dict[str, Any]) -> dict[str, Any]:
        path_id = int(path_row["id"])
        receipt = self._find_receipt(manifest["id"], path_id)

        evidence = scan_path(path_row, self._blocked_partitions.get(path_id, []))

        # 扫描中断（分区读不到）：路径保持等待重试，不能算作已接入。
        if not evidence["readable"]:
            self._upsert_receipt(
                manifest=manifest,
                path_row=path_row,
                scan_evidence=evidence,
                status=RECEIPT_WAITING,
                message="分区读取中断，路径等待重试",
            )
            path_row["status"] = STATUS_WAITING
            path_row["pending"] = True
            path_row["abnormal"] = True
            return {
                "ok": False,
                "path_id": path_id,
                "storage": path_row.get("存储位置", ""),
                "scan_version": evidence["scan_version"],
                "blocked_partitions": evidence["blocked_partitions"],
                "message": "分区读取中断，路径等待重试，不计入已接入",
            }

        # 同一路径同一扫描版本已确认接入：只核对来源，不重复登记，不新增副本。
        if (
            receipt is not None
            and receipt.get("status") == RECEIPT_SUCCESS
            and receipt.get("扫描版本号") == evidence["scan_version"]
            and receipt.get("源编号")
        ):
            return {
                "ok": True,
                "path_id": path_id,
                "storage": path_row.get("存储位置", ""),
                "scan_version": evidence["scan_version"],
                "source_no": receipt["源编号"],
                "source_id": receipt.get("source_id"),
                "schema_fields": evidence["schema_fields"],
                "file_count": evidence["file_count"],
                "file_coverage": evidence["file_coverage"],
                "deduped": True,
                "message": "同一扫描版本已确认接入，跳过重复登记",
            }

        # 实际创建/更新可供联邦查询的数据源（按 连接地址+源类型 幂等）。
        source_id, source_no, created = self._upsert_federation_source(path_row, evidence, manifest)

        # 首次接入的来源才补建语义模型，重试不重复创建。
        if created:
            _create_semantic_model(source_id, source_no, path_row, evidence["schema_fields"])

        # 回写扫描回执清单：成功条目必须带扫描所得 Schema 与文件覆盖范围等独立完整证据。
        self._upsert_receipt(
            manifest=manifest,
            path_row=path_row,
            scan_evidence=evidence,
            status=RECEIPT_SUCCESS,
            message="扫描成功并完成数据源注册",
            source_id=source_id,
            source_no=source_no,
        )

        # 同步更新湖仓路径自身的扫描信息。
        path_row["Schema推导"] = ", ".join(evidence["schema_fields"])
        path_row["文件列表"] = evidence["file_coverage"]
        path_row["最后扫描"] = time.strftime("%Y-%m-%d %H:%M:%S")
        path_row["数据量"] = f"{evidence['file_count']} files"
        path_row["status"] = "已注册"
        path_row["pending"] = False
        path_row["abnormal"] = False

        return {
            "ok": True,
            "path_id": path_id,
            "storage": path_row.get("存储位置", ""),
            "scan_version": evidence["scan_version"],
            "source_id": source_id,
            "source_no": source_no,
            "source_created": created,
            "schema_fields": evidence["schema_fields"],
            "file_count": evidence["file_count"],
            "file_coverage": evidence["file_coverage"],
            "deduped": False,
            "message": "扫描成功，来源已注册并回写回执",
        }

    # ------------------------------------------------------------------
    # 数据源注册（幂等：同一物理路径只创建一个联邦来源）
    # ------------------------------------------------------------------
    def _upsert_federation_source(
        self,
        path_row: dict[str, Any],
        evidence: dict[str, Any],
        manifest: dict[str, Any],
    ) -> tuple[int, str, bool]:
        storage = path_row.get("存储位置", "")
        fmt = path_row.get("文件格式", "parquet")
        rows = store.rows("source_registry")
        existing = next(
            (r for r in rows
             if r.get("连接地址") == storage and r.get("源类型") == fmt),
            None,
        )
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        if existing is not None:
            # 已存在则更新为可用的联邦查询来源，而不是再插一条。
            existing["接入状态"] = "已接入"
            existing["最近同步"] = now
            existing["status"] = "已注册"
            existing["pending"] = False
            existing["abnormal"] = False
            existing["来源批次"] = manifest["清单编号"]
            existing["扫描版本号"] = evidence["scan_version"]
            return int(existing["id"]), str(existing["源编号"]), False

        source_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        source_no = f"SOUR-B{source_id:04d}"
        rows.append({
            "id": source_id,
            "源编号": source_no,
            "源类型": fmt,
            "连接地址": storage,
            "驱动配置": f"lakehouse/{fmt}",
            "数据库名": storage.rsplit("/", 1)[-1],
            "认证方式": "lakehouse_iam",
            "接入状态": "已接入",
            "最近同步": now,
            "来源批次": manifest["清单编号"],
            "扫描版本号": evidence["scan_version"],
            "status": "已注册",
            "pending": False,
            "abnormal": False,
        })
        return source_id, source_no, True

    # ------------------------------------------------------------------
    # 扫描回执清单
    # ------------------------------------------------------------------
    def _find_receipt(self, manifest_id: int, path_id: int) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows(RECEIPT_MODULE)
             if r.get("manifest_id") == manifest_id and r.get("path_id") == path_id),
            None,
        )

    def _upsert_receipt(
        self,
        *,
        manifest: dict[str, Any],
        path_row: dict[str, Any],
        scan_evidence: dict[str, Any] | None,
        status: str,
        message: str,
        source_id: int | None = None,
        source_no: str | None = None,
    ) -> dict[str, Any]:
        path_id = int(path_row["id"])
        existing = self._find_receipt(manifest["id"], path_id)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        if existing is not None:
            receipt = existing
        else:
            rows = store.rows(RECEIPT_MODULE)
            receipt = {
                "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
                "manifest_id": manifest["id"],
                "path_id": path_id,
            }
            rows.append(receipt)

        receipt.update({
            "回执编号": f"RCV-M{manifest['id']:04d}-P{path_id:04d}",
            "清单编号": manifest["清单编号"],
            "数据集": manifest.get("dataset", ""),
            "存储位置": path_row.get("存储位置", ""),
            "文件格式": path_row.get("文件格式", ""),
            "登记结果": status,
            "失败原因": "",
            "status": status,
            "pending": status != RECEIPT_SUCCESS,
            "abnormal": status == RECEIPT_WAITING,
            "更新时间": now,
        })
        if scan_evidence is not None:
            receipt["扫描版本号"] = scan_evidence["scan_version"]
        if scan_evidence is not None and scan_evidence["readable"]:
            receipt["Schema字段"] = ", ".join(scan_evidence["schema_fields"])
            receipt["文件覆盖范围"] = scan_evidence["file_coverage"]
            receipt["文件数"] = str(scan_evidence["file_count"])
            receipt["扫描证据"] = scan_evidence["evidence_id"]
        if status == RECEIPT_WAITING:
            # 等待重试的路径不出具成功证据，清空任何历史成功字段，防止被误判为已接入。
            receipt["Schema字段"] = ""
            receipt["文件覆盖范围"] = ""
            receipt["文件数"] = ""
            receipt["扫描证据"] = ""
        if source_id is not None:
            receipt["source_id"] = source_id
            receipt["源编号"] = source_no
        if status == RECEIPT_WAITING and scan_evidence is not None:
            receipt["失败原因"] = "、".join(scan_evidence["blocked_partitions"])
        if status == RECEIPT_PENDING:
            receipt["失败原因"] = message
        return receipt

    def _write_batch_audit(
        self,
        manifest: dict[str, Any],
        item_results: list[dict[str, Any]],
        is_retry: bool,
    ) -> None:
        rows = store.rows("query_audit")
        audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        rows.append({
            "id": audit_id,
            "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
            "查询语句": (
                f"LAKE_BATCH_{'RETRY' if is_retry else 'SUBMIT'} "
                f"manifest={manifest['清单编号']} attempt={int(manifest.get('重试次数', 0)) + 1}"
            ),
            "执行耗时": "680ms",
            "扫描行数": str(len(item_results)),
            "返回行数": str(sum(1 for r in item_results if r["ok"])),
            "数据来源": BATCH_MODULE,
            "操作用户": "lake_batch_register",
            "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "正常" if manifest["status"] == STATUS_AVAILABLE else "慢查询",
            "pending": manifest["status"] != STATUS_AVAILABLE,
            "abnormal": manifest["status"] == STATUS_WAITING,
        })


class LakeScanReceiptService(BaseModuleService):
    """扫描回执清单：逐条登记结果的回写载体，只随批量清单流转，不单独承接业务动作。"""

    MODULE = RECEIPT_MODULE
    ENTITY = "湖仓扫描回执"
    OPERATION_TERM = "回写"
    LIST_FIELDS = [
        "回执编号", "清单编号", "数据集", "存储位置", "文件格式",
        "扫描版本号", "Schema字段", "文件覆盖范围", "源编号", "登记结果", "失败原因",
    ]
    REQUIRED_FIELDS = ["回执编号", "清单编号", "存储位置"]
    STATUS_ORDER = [RECEIPT_PENDING, RECEIPT_WAITING, RECEIPT_SUCCESS]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        return None, "扫描回执只随批量登记清单回写，不支持单独动作"


# ----------------------------------------------------------------------
# 路径扫描：确定性地生成文件清单、Schema 与扫描版本号
# ----------------------------------------------------------------------
PARTITION_DATES = ["2026-11-01", "2026-11-02", "2026-11-03"]

_FORMAT_SCHEMAS = {
    "parquet": ["uid", "action", "action_time", "platform", "version", "event_type", "properties"],
    "orc": ["id", "user_id", "order_amount", "order_time", "region", "category"],
    "json": ["event_id", "event_type", "event_time", "payload", "source"],
    "csv": ["col_1", "col_2", "col_3", "col_4", "col_5"],
    "avro": ["user_id", "session_id", "page", "duration", "actions", "device"],
}


def _split_paths(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple)):
        return [str(p).strip() for p in raw if str(p).strip()]
    return [part.strip() for part in str(raw or "").replace("\n", ",").split(",") if part.strip()]


def _resolve_path_rows(storage_paths: list[str]) -> list[dict[str, Any]]:
    lake_rows = store.rows("data_lake_read")
    resolved: list[dict[str, Any]] = []
    for path in storage_paths:
        row = next((r for r in lake_rows if r.get("存储位置") == path), None)
        if row is not None:
            resolved.append(row)
    return resolved


def _partition_file_count(path_id: int, dt: str) -> int:
    """同一路径同一分区的文件数稳定，保证重试时扫描版本号一致、可辨识。"""
    digest = hashlib.md5(f"{path_id}|{dt}".encode()).hexdigest()
    return 1 + int(digest[:8], 16) % 5  # 1..5 个文件


def _scan_version(path_id: int, storage: str, fmt: str, schema: list[str]) -> str:
    digest = hashlib.md5(
        "|".join([str(path_id), storage, fmt, ",".join(schema)]).encode()
    ).hexdigest()
    return f"svc-{digest[:12]}"


def scan_path(path_row: dict[str, Any], blocked_partitions: list[str]) -> dict[str, Any]:
    """模拟扫描单条湖仓路径，返回文件清单、字段清单、覆盖范围与扫描版本号。"""
    path_id = int(path_row["id"])
    storage = str(path_row.get("存储位置", ""))
    fmt = str(path_row.get("文件格式", "parquet"))
    schema = list(_FORMAT_SCHEMAS.get(fmt, ["field_1", "field_2", "field_3"]))

    blocked = [dt for dt in PARTITION_DATES if dt in blocked_partitions]
    if blocked:
        # 扫描中断：不出具任何成功证据，调用方必须把该路径置为等待重试。
        return {
            "readable": False,
            "scan_version": _scan_version(path_id, storage, fmt, schema),
            "blocked_partitions": blocked,
            "schema_fields": [],
            "files": [],
            "file_coverage": "",
            "file_count": 0,
            "evidence_id": "",
        }

    files: list[str] = []
    coverage: list[str] = []
    for dt in PARTITION_DATES:
        count = _partition_file_count(path_id, dt)
        coverage.append(f"dt={dt}[{count} files]")
        for i in range(count):
            files.append(f"{storage}/dt={dt}/part-{i:05d}.{fmt}")

    scan_version = _scan_version(path_id, storage, fmt, schema)
    evidence_id = f"EVD-{hashlib.md5(('|'.join(files)).encode()).hexdigest()[:12]}"
    return {
        "readable": True,
        "scan_version": scan_version,
        "blocked_partitions": [],
        "schema_fields": schema,
        "files": files,
        "file_coverage": f"{storage} :: " + ", ".join(coverage),
        "file_count": len(files),
        "evidence_id": evidence_id,
    }


def _create_semantic_model(
    source_id: int, source_no: str, path_row: dict[str, Any], schema: list[str]
) -> None:
    rows = store.rows("semantic_model")
    model_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    storage = str(path_row.get("存储位置", ""))
    rows.append({
        "id": model_id,
        "模型编号": f"SEMA-B{source_id:04d}",
        "模型名称": f"湖仓批量登记_{source_no}",
        "源表映射": f"source_registry:{source_id}",
        "字段别名": ", ".join(schema),
        "度量定义": "COUNT(*), SUM(metrics)",
        "维度定义": ", ".join(schema[:3]),
        "计算口径": "湖仓批量扫描推导",
        "版本序号": "v1.0-batch",
        "status": "已投产",
        "pending": True,
        "abnormal": False,
    })
