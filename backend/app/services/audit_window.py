"""时段窗口取数：按执行事件时间过滤审计记录，供报告生成、版本重放与详情定位共用。

口径约定：
- 执行事件时间（查询时间）决定记录所属窗口；
- 窗口为半开区间 [起点, 终点)，相邻时段跨天共用的边界时刻只计入后一期；
- 迟到补录区分事件时间与到达时间，历史记录缺省到达时间时按事件时间处理；
- 结算按记录编号去重，每个执行编号在窗口总量中只计一次。
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

from app.store import store

_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d",
)


def parse_moment(value: Any) -> datetime | None:
    """把多种写法的时刻解析为 datetime；无法解析时返回 None。"""
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in _TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def canonical(moment: datetime) -> str:
    """统一窗口与水位标签格式，保证同一时段的不同写法落到同一份报告。"""
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def event_time_of(record: dict[str, Any]) -> datetime | None:
    """执行事件时间：决定记录所属窗口。"""
    return parse_moment(record.get("查询时间"))


def arrival_time_of(record: dict[str, Any]) -> datetime | None:
    """到达时间：补录记录进入审计的时刻；历史记录缺省时按事件时间处理。"""
    return parse_moment(record.get("到达时间")) or event_time_of(record)


def select_window_records(
    start: datetime,
    end: datetime,
    arrival_watermark: datetime | None = None,
) -> list[dict[str, Any]]:
    """筛选窗口 [start, end) 内的审计记录，并按记录编号去重。

    arrival_watermark 用于重放某个修正版本的取数口径：只取到达时间不晚于
    水位的记录，使已领取版本的入选集合可以原样复现。
    """
    seen: dict[str, dict[str, Any]] = {}
    for row in store.rows("query_audit"):
        event = event_time_of(row)
        if event is None or not (start <= event < end):
            continue
        arrival = arrival_time_of(row)
        if arrival is None:
            continue
        if arrival_watermark is not None and arrival > arrival_watermark:
            continue
        key = str(row.get("记录编号") or f"row-{row.get('id')}")
        current = seen.get(key)
        if current is None:
            seen[key] = row
        else:
            current_arrival = arrival_time_of(current)
            if current_arrival is None or arrival >= current_arrival:
                seen[key] = row
    return sorted(
        seen.values(),
        key=lambda r: (event_time_of(r) or start, str(r.get("记录编号") or "")),
    )


def summarize_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    """聚合窗口记录：分子（慢查询）、分母（执行总量）与参与计算的审计编号。"""
    slow = [row for row in records if row.get("status") == "慢查询"]
    terminated = [row for row in records if row.get("status") == "已终止"]
    watermark = max((arrival_time_of(row) for row in records if arrival_time_of(row)), default=None)
    return {
        "total": len(records),
        "slow": len(slow),
        "terminated": len(terminated),
        "ratio": round(len(slow) / len(records), 4) if records else 0.0,
        "audit_ids": [str(row.get("记录编号") or "") for row in records],
        "slow_ids": [str(row.get("记录编号") or "") for row in slow],
        "watermark": canonical(watermark) if watermark else "",
    }


def window_fingerprint(start: datetime, end: datetime, summary: dict[str, Any]) -> str:
    """窗口结算口径指纹：窗口边界 + 入选编号集合 + 到达水位。

    指纹一致说明取数口径没有变化，重复或并发提交不应再生效一次。
    """
    payload = "|".join([
        canonical(start),
        canonical(end),
        ",".join(sorted(summary["audit_ids"])),
        summary["watermark"],
    ])
    return hashlib.sha1(payload.encode()).hexdigest()[:16]
