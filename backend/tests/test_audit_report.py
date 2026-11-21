"""时段审计报告端到端验证：窗口取数、边界归属、迟到补录、版本修正与集市交付。"""
import threading

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store
from app.services.query_audit import QueryAuditService

client = TestClient(app)

# 首尾相接的两个时段：跨天共用的边界时刻 2026-11-21 00:00:00
W1_START = "2026-11-20"
W1_END = "2026-11-21"
W2_END = "2026-11-22"


@pytest.fixture(autouse=True)
def isolate_tables():
    """隔离本文件用例写入的三张表，结束后还原，避免串扰其他用例。"""
    names = ("query_audit", "audit_report", "result_mart")
    snapshot = {name: [dict(row) for row in store.rows(name)] for name in names}
    for name in names:
        store.rows(name).clear()
    yield
    for name in names:
        store.rows(name)[:] = snapshot[name]


def _ingest(record_no: str, event_time: str, elapsed_ms: int, arrival: str = "") -> dict:
    resp = client.post("/api/pipelines/audit-report/ingest", params={
        "record_no": record_no, "query_stmt": f"SELECT /* {record_no} */ 1",
        "elapsed_ms": elapsed_ms, "event_time": event_time, "arrival_time": arrival,
    })
    assert resp.status_code == 200, resp.text
    return resp.json()


def _generate(start: str, end: str) -> dict:
    resp = client.post("/api/pipelines/audit-report/generate",
                       params={"start_time": start, "end_time": end})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _replay(start: str, end: str, version: int = 0) -> dict:
    resp = client.get("/api/pipelines/audit-report/replay",
                      params={"start_time": start, "end_time": end, "version": version})
    assert resp.status_code == 200, resp.text
    return resp.json()


class TestWindowFiltering:
    def test_report_only_counts_window_executions(self):
        """时段检索生效：窗口外的执行量不进入报告与集市交付。"""
        _ingest("QUER-OUT-1", "2026-11-19 23:30:00", 100)   # 窗口前
        _ingest("QUER-W1-001", "2026-11-20 10:00:00", 200)  # 首窗
        _ingest("QUER-W2-001", "2026-11-21 09:00:00", 300)  # 次窗
        _ingest("QUER-OUT-2", "2026-11-22 00:00:00", 100)   # 窗口后

        gen = _generate(W1_START, W1_END)
        report = gen["report"]
        assert report["执行总量"] == "1"
        assert report["参与审计编号"] == "QUER-W1-001"

        # 集市交付明细同样只含窗口内执行量，并附参与计算的审计编号
        mart = gen["mart"]
        assert mart["行数统计"] == "1"
        assert mart["参与审计编号"] == "QUER-W1-001"
        assert mart["窗口开始"] == "2026-11-20 00:00:00"
        assert mart["窗口结束"] == "2026-11-21 00:00:00"

    def test_boundary_moment_counts_once_in_later_window(self):
        """相邻时段跨天共用的边界时刻只计入后一期。"""
        _ingest("QUER-B-1", "2026-11-21 00:00:00", 100)  # 恰好在边界上

        first = _generate(W1_START, W1_END)
        assert first["report"]["执行总量"] == "0"
        assert first["report"]["参与审计编号"] == ""

        second = _generate(W1_END, W2_END)
        assert second["report"]["执行总量"] == "1"
        assert second["report"]["参与审计编号"] == "QUER-B-1"

        # 两个窗口的结算总量合计不重复计数
        total = int(first["report"]["执行总量"]) + int(second["report"]["执行总量"])
        assert total == 1


class TestLateArrivalCorrection:
    def test_full_late_arrival_scenario(self):
        """首窗关账领取后收到迟到执行：隔天重查出修正版本，已领取版本可重放对账。"""
        # 建立首尾相接的两个时段，各放入一笔执行
        _ingest("QUER-W1-001", "2026-11-20 10:00:00", 200, arrival="2026-11-20 10:01:00")
        _ingest("QUER-W2-001", "2026-11-21 09:00:00", 300, arrival="2026-11-21 09:01:00")

        # 首窗报告生成并发布：发布结果回写报告清单
        gen1 = _generate(W1_START, W1_END)
        v1 = gen1["report"]
        assert v1["修正版本"] == "1"
        assert v1["执行总量"] == "1" and v1["慢查询量"] == "0"
        assert v1["慢查询占比"] == "0.0"
        assert v1["发布集市"] and v1["status"] == "已发布"
        mart_no = v1["发布集市"]

        # 首窗关账：从集市领取
        claim = client.post(f"/api/pipelines/audit-report/{v1['id']}/claim")
        assert claim.status_code == 200 and claim.json()["ok"]
        assert store.find("audit_report", v1["id"])["领取时间"]
        mart = next(m for m in store.rows("result_mart") if m["集市编号"] == mart_no)
        assert mart["交付状态"] == "已领取"

        # 审计收到一笔属于首窗的迟到执行（慢查询）：事件时间在首窗，到达时间隔天
        late = _ingest("QUER-W1-002", "2026-11-20 22:00:00", 8000, arrival="2026-11-22 08:00:00")
        assert late["late"] and late["record"]["status"] == "慢查询"
        assert late["record"]["查询时间"] == "2026-11-20 22:00:00"
        assert late["record"]["到达时间"] == "2026-11-22 08:00:00"

        # 隔天再查首窗：迟到记录触发修正版本 2
        gen2 = _generate(W1_START, W1_END)
        v2 = gen2["report"]
        assert v2["修正版本"] == "2"
        assert v2["执行总量"] == "2" and v2["慢查询量"] == "1"
        assert v2["慢查询占比"] == "0.5"
        assert v2["慢查询编号"] == "QUER-W1-002"
        assert v2["发布集市"] and v2["发布集市"] != mart_no

        # 旧版本转入已修正，领取时间保留供对账
        old = store.find("audit_report", v1["id"])
        assert old["status"] == "已修正" and old["领取时间"]

        # 用已领取的版本重放当时的查询数（对账）
        replay_v1 = _replay(W1_START, W1_END, version=1)
        assert replay_v1["执行总量"] == "1" and replay_v1["慢查询量"] == "0"
        assert replay_v1["分母编号"] == ["QUER-W1-001"]
        assert replay_v1["分子编号"] == []
        assert replay_v1["领取时间"]

        # 最新口径：分子分母都能追到执行编号
        replay_now = _replay(W1_START, W1_END)
        assert replay_now["修正版本"] == "2"
        assert replay_now["分母编号"] == ["QUER-W1-001", "QUER-W1-002"]
        assert replay_now["分子编号"] == ["QUER-W1-002"]

        # 从报告详情按执行窗口定位入选记录：v2 含补录，v1 按水位排除补录
        detail_v2 = client.get(f"/api/pipelines/audit-report/{v2['id']}/detail").json()
        assert {r["记录编号"] for r in detail_v2["入选记录"]} == {"QUER-W1-001", "QUER-W1-002"}
        assert detail_v2["集市交付"]["集市编号"] == v2["发布集市"]

        detail_v1 = client.get(f"/api/pipelines/audit-report/{v1['id']}/detail").json()
        assert {r["记录编号"] for r in detail_v1["入选记录"]} == {"QUER-W1-001"}

        # 次窗不受首窗补录影响
        gen_w2 = _generate(W1_END, W2_END)
        assert gen_w2["report"]["参与审计编号"] == "QUER-W2-001"

    def test_duplicate_backfill_counts_execution_once(self):
        """每个执行编号在结算总量中只计一次：入账去重 + 聚合去重双保险。"""
        _ingest("QUER-DUP-1", "2026-11-20 10:00:00", 100)
        dup = _ingest("QUER-DUP-1", "2026-11-20 10:00:00", 100)
        assert dup["deduplicated"]
        assert len([r for r in store.rows("query_audit") if r["记录编号"] == "QUER-DUP-1"]) == 1

        # 即使仓库混入同编号脏数据，聚合结算仍只计一次
        store.rows("query_audit").append({
            "id": 999, "记录编号": "QUER-DUP-1", "查询语句": "SELECT 1",
            "执行耗时": "100ms", "扫描行数": "1", "返回行数": "1",
            "数据来源": "query_federation", "操作用户": "analyst",
            "查询时间": "2026-11-20 10:00:00", "到达时间": "2026-11-20 10:02:00",
            "status": "正常", "pending": False, "abnormal": False,
        })
        gen = _generate(W1_START, W1_END)
        assert gen["report"]["执行总量"] == "1"
        assert gen["report"]["参与审计编号"] == "QUER-DUP-1"


class TestConcurrentSubmission:
    def test_same_window_submission_takes_effect_once(self):
        """并发提交同一窗口的报告只能生效一次。"""
        _ingest("QUER-C-1", "2026-11-20 10:00:00", 100)
        svc = QueryAuditService()
        results: list[dict] = []

        def submit():
            results.append(svc.generate_performance_report(W1_START, W1_END))

        threads = [threading.Thread(target=submit) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r["ok"] for r in results)
        assert sum(1 for r in results if not r["deduplicated"]) == 1
        assert len(store.rows("audit_report")) == 1
        # 集市也只交付一次
        deliveries = [m for m in store.rows("result_mart") if m.get("来源报告")]
        assert len(deliveries) == 1

    def test_sequential_resubmit_is_deduplicated(self):
        """口径未变的重复提交返回已生效报告，不产生新版本。"""
        _ingest("QUER-S-1", "2026-11-20 10:00:00", 100)
        first = _generate(W1_START, W1_END)
        second = _generate("2026-11-20 00:00:00", "2026-11-21 00:00:00")  # 同一时段不同写法
        assert second["deduplicated"]
        assert second["report"]["报告编号"] == first["report"]["报告编号"]
        assert len(store.rows("audit_report")) == 1
