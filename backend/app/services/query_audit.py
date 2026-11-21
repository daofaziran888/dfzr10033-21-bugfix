"""查询审计领域服务：查询日志、慢查询分析、执行性能追踪与跨模块审计串联。"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime
from typing import Any

from app.services import audit_window
from app.services.audit_report import AuditReportService
from app.services.base import BaseModuleService
from app.services.result_mart import ResultMartService
from app.store import store

# 同一窗口的报告生成串行化：并发提交同一窗口只能生效一次。
_REPORT_LOCK = threading.Lock()


class QueryAuditService(BaseModuleService):
    MODULE = "query_audit"
    ENTITY = "审计记录"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["记录编号", "查询语句", "执行耗时", "扫描行数", "返回行数", "数据来源", "操作用户", "查询时间"]
    REQUIRED_FIELDS = ["记录编号", "查询语句"]
    STATUS_ORDER = ["正常", "慢查询", "已终止", "已审计"]
    ACTION_RULES = {"标记慢查询": "慢查询", "终止查询": "已终止", "标记审计": "已审计"}
    NEGATIVE_ACTIONS = ["标记慢查询", "终止查询"]

    def analyze_slow_queries(self, threshold_ms: int = 5000) -> dict[str, Any]:
        """分析慢查询：扫描 query_audit，生成缓存预热建议，记录成本浪费。"""
        records = store.rows(self.MODULE)
        slow = []
        by_source: dict[str, int] = {}

        for record in records:
            elapsed_str = record.get("执行耗时", "0ms")
            try:
                elapsed = float(elapsed_str.replace("ms", ""))
            except (ValueError, AttributeError):
                elapsed = 0

            if elapsed > threshold_ms:
                slow.append({
                    "id": record["id"],
                    "编号": record.get("记录编号"),
                    "语句摘要": (record.get("查询语句", ""))[:60],
                    "耗时": elapsed_str,
                    "来源": record.get("数据来源"),
                })
                source = record.get("数据来源", "unknown")
                by_source[source] = by_source.get(source, 0) + 1

        # 生成预热建议
        warmup_recommendations = []
        for source, count in by_source.items():
            if count >= 2:
                warmup_recommendations.append({
                    "source": source,
                    "slow_count": count,
                    "action": "PREWARM_CACHE",
                })

        # 写入成本治理记录
        if slow:
            _record_slow_query_cost(len(slow), threshold_ms)

        return {
            "ok": True,
            "threshold_ms": threshold_ms,
            "total_records": len(records),
            "slow_count": len(slow),
            "by_source": by_source,
            "warmup_recommendations": warmup_recommendations,
            "top_slow_queries": slow[:10],
        }

    def get_query_profile(self, record_id: int) -> dict[str, Any]:
        """获取查询画像：关联 result_cache 和 cost_govern。"""
        record = store.find(self.MODULE, record_id)
        if record is None:
            return {"ok": False, "message": "审计记录不存在"}

        # 检查是否有缓存结果
        cache_hit = None
        caches = store.rows("result_cache")
        for c in caches:
            if record.get("记录编号", "") in str(c.get("查询摘要", "")):
                cache_hit = {
                    "cache_id": c["id"],
                    "status": c.get("status"),
                    "hits": c.get("缓存命中"),
                }
                break

        # 检查成本配额
        cost_records = []
        cost_rows = store.rows("cost_govern")
        for c in cost_rows:
            if record.get("数据来源", "") in str(c.get("适用角色", "")):
                cost_records.append({
                    "cost_id": c["id"],
                    "strategy": c.get("策略编号"),
                    "quota": c.get("查询上限"),
                    "status": c.get("status"),
                })

        # 更新审计记录
        record["status"] = "已审计"
        record["pending"] = False

        return {
            "ok": True,
            "record_id": record_id,
            "query": (record.get("查询语句", ""))[:100],
            "elapsed": record.get("执行耗时"),
            "scanned": record.get("扫描行数"),
            "returned": record.get("返回行数"),
            "cache_hit": cache_hit,
            "cost_records": cost_records,
        }

    def ingest_execution(self, values: dict[str, Any], *, arrival_time: str | None = None) -> dict[str, Any]:
        """接收一笔执行审计：事件时间决定所属窗口，到达时间用于迟到版本修正。

        补录须区分事件时间（查询时间）与到达时间；同一执行编号只入账一次，
        重复补录直接返回原记录，保证结算总量中每个执行编号只计一次。
        """
        event = audit_window.parse_moment(values.get("查询时间"))
        if event is None:
            return {"ok": False, "message": "缺少可解析的查询时间（执行事件时间）"}
        arrival = audit_window.parse_moment(arrival_time) if arrival_time else None
        arrival_dt = arrival or datetime.now()
        arrival_label = audit_window.canonical(arrival_dt)

        record_no = str(values.get("记录编号") or "").strip()
        rows = store.rows(self.MODULE)
        if record_no:
            for row in rows:
                if str(row.get("记录编号")) == record_no:
                    return {"ok": True, "deduplicated": True, "late": False,
                            "message": f"执行编号{record_no}已入账，重复补录被去重", "record": row}
        else:
            record_no = f"QUER-{uuid.uuid4().hex[:8].upper()}"

        elapsed_text = str(values.get("执行耗时") or "0ms")
        try:
            elapsed = float(elapsed_text.replace("ms", ""))
        except (ValueError, AttributeError):
            elapsed = 0.0
        status = str(values.get("status") or "").strip() or ("慢查询" if elapsed > 5000 else "正常")
        record = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "记录编号": record_no,
            "查询语句": values.get("查询语句", ""),
            "执行耗时": elapsed_text,
            "扫描行数": str(values.get("扫描行数", "0")),
            "返回行数": str(values.get("返回行数", "0")),
            "数据来源": values.get("数据来源", "query_federation"),
            "操作用户": values.get("操作用户", "analyst"),
            "查询时间": audit_window.canonical(event),
            "到达时间": arrival_label,
            "status": status,
            "pending": False,
            "abnormal": status == "慢查询",
        }
        rows.append(record)
        late = arrival_dt > event
        return {"ok": True, "deduplicated": False, "late": late, "record": record}

    def generate_performance_report(self, start_time: str, end_time: str) -> dict[str, Any]:
        """生成时段审计汇总：按执行事件时间过滤窗口内审计记录并聚合，登记修正版本后发布集市。

        窗口为半开区间 [起点, 终点)，相邻时段跨天共用的边界时刻只计入后一期；
        迟到补录按到达时间推高水位并触发新修正版本；口径相同的重复/并发提交只生效一次。
        """
        start = audit_window.parse_moment(start_time)
        end = audit_window.parse_moment(end_time)
        if start is None or end is None:
            return {"ok": False, "message": f"无法解析时段：{start_time} ~ {end_time}"}
        if not start < end:
            return {"ok": False, "message": "时段起点必须早于终点（半开区间 [起点, 终点)）"}

        with _REPORT_LOCK:
            start_label = audit_window.canonical(start)
            end_label = audit_window.canonical(end)
            records = audit_window.select_window_records(start, end)
            summary = audit_window.summarize_records(records)
            fingerprint = audit_window.window_fingerprint(start, end, summary)

            registry = AuditReportService()
            latest = registry.latest_for_window(start_label, end_label)
            if latest is not None and latest.get("口径指纹") == fingerprint:
                return {"ok": True, "deduplicated": True,
                        "message": "同一窗口相同口径的报告已生效，重复提交被去重",
                        "report": latest, "summary": summary}

            report = registry.register_version(
                start_label=start_label, end_label=end_label,
                summary=summary, fingerprint=fingerprint)
            publish = ResultMartService().publish_audit_summary(report)

        return {
            "ok": True,
            "deduplicated": False,
            "report": report,
            "summary": summary,
            "mart": publish["mart"],
            "message": f"窗口{report['统计时段']}第{report['修正版本']}版报告已生成并发布到{publish['mart']['集市编号']}",
        }


def _record_slow_query_cost(slow_count: int, threshold_ms: int) -> None:
    rows = store.rows("cost_govern")
    cost_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    cost = {
        "id": cost_id,
        "策略编号": f"COST-SQ{uuid.uuid4().hex[:8]}",
        "适用角色": "slow_query_penalty",
        "查询上限": str(slow_count),
        "并发上限": str(threshold_ms),
        "月度配额": str(slow_count * 30),
        "告警阈值": "5",
        "限流动作": "throttle" if slow_count > 10 else "monitor",
        "生效时段": time.strftime("%Y-%m-%d"),
        "status": "已告警" if slow_count > 5 else "已生效",
        "pending": slow_count > 5,
        "abnormal": slow_count > 20,
    }
    rows.append(cost)