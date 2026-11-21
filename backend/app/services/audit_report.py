"""时段审计报告清单：窗口报告的版本登记、领取对账与按执行窗口定位入选记录。

数据流：query_audit 生成时段汇总 → 本模块登记修正版本（报告清单）→
result_mart 发布交付并回写发布结果。每个窗口的每个修正版本都可追查
窗口边界、参与计算的审计编号与领取记录。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import audit_window
from app.services.base import BaseModuleService
from app.store import store


class AuditReportService(BaseModuleService):
    MODULE = "audit_report"
    ENTITY = "时段审计报告"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["报告编号", "统计时段", "修正版本", "执行总量", "慢查询量", "慢查询占比", "发布集市", "生成时间"]
    REQUIRED_FIELDS = ["报告编号", "统计时段"]
    STATUS_ORDER = ["已生成", "已发布", "已领取", "已修正"]
    ACTION_RULES = {"领取报告": "已领取"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """工作台动作直连真实领取逻辑：回写领取时间并同步集市交付状态。"""
        if action == "领取报告":
            result = self.claim_report(entry_id)
            entry = store.find(self.MODULE, entry_id)
            return (entry, result["message"]) if result["ok"] else (None, result["message"])
        return super().run_action(entry_id, action)

    # -- 版本注册 --

    def versions_for_window(self, start_label: str, end_label: str) -> list[dict[str, Any]]:
        """同一执行窗口的全部修正版本，按登记顺序排列。"""
        rows = [
            row for row in store.rows(self.MODULE)
            if row.get("窗口开始") == start_label and row.get("窗口结束") == end_label
        ]
        return sorted(rows, key=lambda row: int(row.get("id", 0)))

    def latest_for_window(self, start_label: str, end_label: str) -> dict[str, Any] | None:
        versions = self.versions_for_window(start_label, end_label)
        return versions[-1] if versions else None

    def register_version(
        self,
        *,
        start_label: str,
        end_label: str,
        summary: dict[str, Any],
        fingerprint: str,
    ) -> dict[str, Any]:
        """登记新修正版本：旧版本转入已修正（领取时间保留，供对账重放）。"""
        rows = store.rows(self.MODULE)
        previous = self.latest_for_window(start_label, end_label)
        if previous is not None:
            previous["status"] = "已修正"
            previous["pending"] = False
        new_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        version = int(previous.get("修正版本", "0")) + 1 if previous is not None else 1
        report = {
            "id": new_id,
            "报告编号": f"AUDR-{new_id:04d}",
            "统计时段": f"{start_label} ~ {end_label}",
            "窗口开始": start_label,
            "窗口结束": end_label,
            "修正版本": str(version),
            "执行总量": str(summary["total"]),
            "慢查询量": str(summary["slow"]),
            "慢查询占比": str(summary["ratio"]),
            "终止查询量": str(summary["terminated"]),
            "参与审计编号": ",".join(summary["audit_ids"]),
            "慢查询编号": ",".join(summary["slow_ids"]),
            "到达水位": summary["watermark"],
            "口径指纹": fingerprint,
            "发布集市": "",
            "领取时间": "",
            "生成时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "已生成",
            "pending": True,
            "abnormal": False,
        }
        rows.append(report)
        return report

    # -- 领取对账 --

    def claim_report(self, report_id: int) -> dict[str, Any]:
        """从集市领取报告：记录领取时间并同步交付状态，重复领取只生效一次。"""
        report = store.find(self.MODULE, report_id)
        if report is None:
            return {"ok": False, "message": f"时段审计报告 {report_id} 不存在或已归档"}
        if report.get("status") == "已修正":
            return {"ok": False, "message": f"报告{report.get('报告编号')}已被修正版本替代，请领取最新版本"}
        if report.get("领取时间"):
            return {"ok": True, "deduplicated": True, "message": f"报告{report.get('报告编号')}已领取过", "report": report}
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        report["领取时间"] = now
        report["status"] = "已领取"
        report["pending"] = False
        mart_no = report.get("发布集市")
        if mart_no:
            for mart in store.rows("result_mart"):
                if mart.get("集市编号") == mart_no:
                    mart["交付状态"] = "已领取"
                    mart["领取时间"] = now
        return {"ok": True, "deduplicated": False, "message": f"报告{report.get('报告编号')}已从集市领取", "report": report}

    # -- 详情与重放 --

    def report_detail(self, report_id: int) -> dict[str, Any]:
        """报告详情：按执行窗口与版本到达水位定位入选记录，并带出集市交付明细。"""
        report = store.find(self.MODULE, report_id)
        if report is None:
            return {"ok": False, "message": f"时段审计报告 {report_id} 不存在或已归档"}
        start = audit_window.parse_moment(report.get("窗口开始"))
        end = audit_window.parse_moment(report.get("窗口结束"))
        watermark = audit_window.parse_moment(report.get("到达水位"))
        records = audit_window.select_window_records(start, end, arrival_watermark=watermark) if start and end else []
        mart = None
        for row in store.rows("result_mart"):
            if row.get("集市编号") and row.get("集市编号") == report.get("发布集市"):
                mart = row
                break
        return {
            "ok": True,
            "report": report,
            "入选记录": [
                {
                    "id": row.get("id"),
                    "记录编号": row.get("记录编号"),
                    "查询语句": row.get("查询语句"),
                    "执行耗时": row.get("执行耗时"),
                    "查询时间": row.get("查询时间"),
                    "到达时间": row.get("到达时间") or row.get("查询时间"),
                    "status": row.get("status"),
                }
                for row in records
            ],
            "集市交付": mart,
        }

    def replay_window(self, start_time: str, end_time: str, version: int | None = None) -> dict[str, Any]:
        """按窗口重放某个修正版本的结算口径：分子分母都可追到执行编号。

        不传版本时返回最新生效版本；传入已领取版本号即可重放当时的查询数。
        """
        start = audit_window.parse_moment(start_time)
        end = audit_window.parse_moment(end_time)
        if start is None or end is None:
            return {"ok": False, "message": f"无法解析时段：{start_time} ~ {end_time}"}
        versions = self.versions_for_window(audit_window.canonical(start), audit_window.canonical(end))
        if not versions:
            return {"ok": False, "message": "该执行窗口暂无时段审计报告"}
        if version is None:
            target = versions[-1]
        else:
            target = next((row for row in versions if int(row.get("修正版本", "0")) == version), None)
            if target is None:
                return {"ok": False, "message": f"该执行窗口没有修正版本 {version}"}
        return {
            "ok": True,
            "报告编号": target.get("报告编号"),
            "统计时段": target.get("统计时段"),
            "修正版本": target.get("修正版本"),
            "status": target.get("status"),
            "执行总量": target.get("执行总量"),
            "慢查询量": target.get("慢查询量"),
            "慢查询占比": target.get("慢查询占比"),
            "分母编号": [no for no in str(target.get("参与审计编号") or "").split(",") if no],
            "分子编号": [no for no in str(target.get("慢查询编号") or "").split(",") if no],
            "到达水位": target.get("到达水位"),
            "发布集市": target.get("发布集市"),
            "领取时间": target.get("领取时间"),
            "生成时间": target.get("生成时间"),
        }
