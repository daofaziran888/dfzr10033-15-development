"""湖仓批量登记清单验收测试。

场景：对三条路径发起一次批量扫描，其中一条分区读不到；恢复该分区后按同一批次
重新提交整组，核查实际源编号、每条 Schema 字段，并确认原成功条目不产生副本。
"""
from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store
from app.services.lake_batch_register import (
    PARTITION_DATES,
    _FORMAT_SCHEMAS,
)

client = TestClient(app)


def _create_lake_path(path_no: str, storage: str, fmt: str) -> dict:
    resp = client.post("/api/data_lake_read", json={"values": {
        "路径编号": path_no,
        "存储位置": storage,
        "文件格式": fmt,
        "分区模式": "dt",
        "Schema推导": "",
        "文件列表": "",
        "最后扫描": "",
        "数据量": "",
    }})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"], body["message"]
    return body["entry"]


def _sources_for_manifest(manifest_no: str) -> list[dict]:
    return [
        row for row in store.rows("source_registry")
        if row.get("来源批次") == manifest_no
    ]


def _receipts(manifest_id: int) -> list[dict]:
    return [
        row for row in store.rows("lake_scan_receipt")
        if row.get("manifest_id") == manifest_id
    ]


@pytest.fixture
def three_paths():
    # 每次使用唯一存储位置，避免与其他测试已登记的湖仓路径撞名而被复用。
    suffix = uuid.uuid4().hex[:8]
    paths = [
        _create_lake_path(f"DATA-BATCH-{suffix}-P1", f"s3://data-lake/batch/{suffix}/events", "parquet"),
        _create_lake_path(f"DATA-BATCH-{suffix}-P2", f"s3://data-lake/batch/{suffix}/orders", "orc"),
        _create_lake_path(f"DATA-BATCH-{suffix}-P3", f"s3://data-lake/batch/{suffix}/stream", "json"),
    ]
    return paths


@pytest.fixture
def svc():
    # 与路由层共用同一单例，保证故障注入状态在服务调用与 API 调用之间一致。
    from app.routers.resource import lake_batch_svc
    lake_batch_svc._blocked_partitions.clear()
    yield lake_batch_svc
    lake_batch_svc._blocked_partitions.clear()


class TestLakeBatchRegister:
    def test_full_batch_interrupt_and_retry(self, three_paths, svc):
        p1, p2, p3 = three_paths
        manifest_no = "LBR-ACCEPT-0001"

        # 1) 创建清单：三条路径一次性提交
        entry, msg = svc.create_manifest({
            "清单编号": manifest_no,
            "数据集": "batch_demo",
            "来源路径": ", ".join(p["存储位置"] for p in three_paths),
        })
        assert entry is not None, msg
        manifest_id = entry["id"]
        assert entry["条目总数"] == "3"
        # 预生成的回执每条路径一行
        assert len(_receipts(manifest_id)) == 3
        assert all(r["登记结果"] == "待扫描" for r in _receipts(manifest_id))

        # 2) 首次整组扫描：第三条路径的一个分区读不到
        blocked = {p3["id"]: [PARTITION_DATES[1]]}
        first = svc.submit_manifest(manifest_id, blocked_partitions=blocked)
        assert first["ok"]
        assert first["total"] == 3
        assert first["registered"] == 2
        assert first["waiting_retry"] == 1
        assert first["all_available"] is False

        manifest = store.find("lake_batch_register", manifest_id)
        # 中断后不能冒充整组可用
        assert manifest["status"] == "等待重试"
        assert manifest["批次状态"] == "等待重试"
        assert manifest["重试次数"] == "0"

        receipts = _receipts(manifest_id)
        ok_receipts = [r for r in receipts if r["登记结果"] == "已接入"]
        wait_receipts = [r for r in receipts if r["登记结果"] == "等待重试"]
        assert len(ok_receipts) == 2 and len(wait_receipts) == 1

        # 成功条目必须带独立完整的扫描证据：Schema + 文件覆盖范围 + 版本号
        for receipt in ok_receipts:
            assert receipt["扫描版本号"].startswith("svc-")
            assert receipt["扫描证据"]
            assert receipt["文件数"] and int(receipt["文件数"]) > 0
            assert "dt=2026-11-01" in receipt["文件覆盖范围"]
            assert receipt["源编号"]
            expected_fields = _FORMAT_SCHEMAS[receipt["文件格式"]]
            assert receipt["Schema字段"] == ", ".join(expected_fields)

        # 等待重试的分区不能被算作已接入：无来源编号、无 Schema、无覆盖范围
        waiting = wait_receipts[0]
        assert waiting["path_id"] == p3["id"]
        assert not waiting.get("源编号")
        assert waiting["Schema字段"] == ""
        assert waiting["文件覆盖范围"] == ""
        assert PARTITION_DATES[1] in waiting["失败原因"]

        # 实际只创建了两个联邦查询来源，失败路径没有来源
        sources = _sources_for_manifest(manifest_no)
        assert len(sources) == 2
        source_storages = {s["连接地址"] for s in sources}
        assert p3["存储位置"] not in source_storages
        assert {s["接入状态"] for s in sources} == {"已接入"}

        # 3) 恢复失败分区后，按同一批次编号重新提交整组
        svc.restore_partition(p3["id"], [PARTITION_DATES[1]])
        retry = svc.retry_manifest(manifest_id)
        assert retry["ok"]
        assert retry["is_retry"] is True
        assert retry["manifest_id"] == manifest_id
        assert retry["manifest_no"] == manifest_no  # 重试不是新批
        assert retry["registered"] == 3
        assert retry["waiting_retry"] == 0
        assert retry["all_available"] is True

        manifest = store.find("lake_batch_register", manifest_id)
        assert manifest["重试次数"] == "1"
        assert manifest["status"] == "整组可用"

        # 原成功条目走幂等分支，不重复登记
        deduped = {item["path_id"]: item.get("deduped") for item in retry["items"]}
        assert deduped[p1["id"]] is True
        assert deduped[p2["id"]] is True
        assert deduped[p3["id"]] is False

        # 4) 核查实际源编号与每条 Schema 字段：仍是三个来源，无副本
        sources = _sources_for_manifest(manifest_no)
        assert len(sources) == 3
        by_storage = {s["连接地址"]: s for s in sources}
        for path in three_paths:
            source = by_storage[path["存储位置"]]
            assert source["源编号"] == f"SOUR-B{source['id']:04d}"
            # 源编号在回执里一致
            receipt = next(r for r in _receipts(manifest_id) if r["path_id"] == path["id"])
            assert receipt["源编号"] == source["源编号"]
            assert receipt["source_id"] == source["id"]
            assert receipt["Schema字段"] == ", ".join(_FORMAT_SCHEMAS[path["文件格式"]])

        # 回执始终每条路径一行，重试不新增副本
        assert len(_receipts(manifest_id)) == 3

        # 湖仓路径本身也只登记一次：路径状态为已注册
        for path in three_paths:
            row = store.find("data_lake_read", path["id"])
            assert row["status"] == "已注册"
            assert row["abnormal"] is False

        # 5) 整组可用后再次提交应被拒绝，不产生任何新数据
        svc._blocked_partitions.clear()
        before_sources = len(store.rows("source_registry"))
        again = svc.submit_manifest(manifest_id)
        assert again["ok"] is False
        assert len(store.rows("source_registry")) == before_sources
        assert len(_receipts(manifest_id)) == 3

    def test_unknown_path_rejected(self, svc):
        entry, msg = svc.create_manifest({
            "清单编号": "LBR-REJECT-0001",
            "数据集": "batch_demo",
            "来源路径": "s3://data-lake/not-registered/path",
        })
        assert entry is None
        assert "未登记" in msg

    def test_pipeline_api_interrupt_and_retry(self, three_paths, svc):
        p1, p2, p3 = three_paths
        manifest_no = "LBR-API-0001"
        resp = client.post("/api/pipelines/lake-batch-register", params={
            "manifest_no": manifest_no,
            "dataset": "batch_api",
            "storage_paths": ",".join(p["存储位置"] for p in three_paths),
            "blocked": f"{p3['id']}:{PARTITION_DATES[0]}",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"]
        manifest_id = body["manifest"]["id"]
        assert body["submit"]["waiting_retry"] == 1
        assert body["manifest"]["status"] == "等待重试"
        assert len(body["receipts"]) == 3

        # 恢复分区后重试整组
        resp = client.post("/api/pipelines/lake-batch-retry", params={
            "manifest_id": manifest_id,
            "restored": f"{p3['id']}:{PARTITION_DATES[0]}",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["submit"]["all_available"] is True
        assert body["manifest"]["status"] == "整组可用"
        assert body["manifest"]["重试次数"] == "1"
        # 三条回执、三个来源，无副本
        assert len(body["receipts"]) == 3
        assert len(_sources_for_manifest(manifest_no)) == 3
