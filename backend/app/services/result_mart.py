"""结果集市领域服务：数据Mart骨架 + 时段审计汇总发布交付。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ResultMartService(BaseModuleService):
    MODULE = "result_mart"
    ENTITY = "数据Mart"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["集市编号", "集市名称", "数据来源", "主题域", "行数统计", "刷新周期", "下游消费", "最后刷新"]
    REQUIRED_FIELDS = ["集市编号", "集市名称", "数据来源"]
    STATUS_ORDER = ["草稿", "刷新中", "可用", "刷新失败"]
    ACTION_RULES = {"启动刷新": "刷新中", "标记可用": "可用", "标记失败": "刷新失败"}
    NEGATIVE_ACTIONS = []

    def publish_audit_summary(self, report: dict[str, Any]) -> dict[str, Any]:
        """发布时段审计汇总：附上参与计算的审计编号，发布结果回写报告清单。

        行数统计只含窗口内执行量；同一报告版本只交付一次，重复发布返回原交付。
        """
        rows = store.rows(self.MODULE)
        for row in rows:
            if row.get("来源报告") and row.get("来源报告") == report.get("报告编号"):
                return {"ok": True, "deduplicated": True, "mart": row}
        mart_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        mart = {
            "id": mart_id,
            "集市编号": f"RESU-RP{mart_id:04d}",
            "集市名称": f"时段审计汇总_{report.get('统计时段', '')}",
            "数据来源": "query_audit",
            "主题域": "性能分析",
            "行数统计": str(report.get("执行总量", "0")),
            "刷新周期": "daily",
            "下游消费": "monitoring",
            "最后刷新": now,
            "窗口开始": report.get("窗口开始", ""),
            "窗口结束": report.get("窗口结束", ""),
            "修正版本": report.get("修正版本", "1"),
            "参与审计编号": report.get("参与审计编号", ""),
            "慢查询编号": report.get("慢查询编号", ""),
            "慢查询占比": report.get("慢查询占比", "0"),
            "来源报告": report.get("报告编号", ""),
            "交付状态": "已交付",
            "领取时间": "",
            "status": "可用",
            "pending": False,
            "abnormal": False,
        }
        rows.append(mart)
        # 发布结果回写报告清单
        report["发布集市"] = mart["集市编号"]
        report["status"] = "已发布"
        report["pending"] = False
        return {"ok": True, "deduplicated": False, "mart": mart}
