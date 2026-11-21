"""水位推进与延迟检测引擎。

追踪每条ETL管线的数据分区消费进度：
- 水位推进（基于调度时间的分区递增）
- 延迟检测（当前水位 vs 期望水位）
- 滑动窗口内的迟到数据处理标记
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any


class WatermarkStatus(str, Enum):
    NORMAL = "正常"
    DELAYED = "延迟"
    SEVERE = "严重延迟"
    PAUSED = "已暂停"


@dataclass
class WatermarkRecord:
    """一条水位追踪记录。"""
    pipeline_id: str
    partition_field: str                     # 分区字段，如 dt
    current_watermark: str                   # 当前水位，如 2026-11-18
    scheduled_time: datetime
    delay_minutes: int = 0
    last_advance_time: datetime | None = None
    status: WatermarkStatus = WatermarkStatus.NORMAL
    partition_format: str = "%Y-%m-%d"      # 分区格式


@dataclass
class WatermarkAdvanceResult:
    """水位推进结果。"""
    pipeline_id: str
    old_watermark: str
    new_watermark: str
    advanced_partitions: list[str]           # 推进过程中经过的分区
    delay_minutes: int
    new_status: WatermarkStatus


class WatermarkEngine:
    """水位推进与延迟检测引擎。

    用法:
        eng = WatermarkEngine()
        rec = WatermarkRecord(
            pipeline_id="p_dwd_order",
            partition_field="dt",
            current_watermark="2026-11-17",
            scheduled_time=datetime(2026, 11, 18, 8, 0),
        )
        result = eng.advance(rec, "2026-11-18")
        print(result.delay_minutes, result.new_status)

        delay_report = eng.compute_delay_metrics([rec, ...])
    """

    def advance(
        self,
        record: WatermarkRecord,
        new_watermark: str,
        *,
        now: datetime | None = None,
    ) -> WatermarkAdvanceResult:
        """将水位推进到 new_watermark，并计算延迟。"""
        now = now or datetime.now()
        old = record.current_watermark

        # 计算通过的分区列表
        partitions = self._enumerate_partitions(
            old, new_watermark, record.partition_format
        )

        # 解析新旧水位为日期
        try:
            new_dt = datetime.strptime(new_watermark, record.partition_format)
        except ValueError:
            new_dt = now

        # 期望水位：基于调度时间+频率
        expected = self._expected_watermark(record.scheduled_time, now)

        # 计算延迟分钟数
        delay = self._compute_delay(
            new_dt, expected
        )

        # 判断状态
        status = WatermarkStatus.NORMAL
        if delay > 120:
            status = WatermarkStatus.SEVERE
        elif delay > 30:
            status = WatermarkStatus.DELAYED

        return WatermarkAdvanceResult(
            pipeline_id=record.pipeline_id,
            old_watermark=old,
            new_watermark=new_watermark,
            advanced_partitions=partitions,
            delay_minutes=delay,
            new_status=status,
        )

    def compute_delay_metrics(
        self,
        records: list[WatermarkRecord],
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """批量计算所有管线的延迟指标。"""
        now = now or datetime.now()
        total = len(records)
        normal = 0
        delayed = 0
        severe = 0
        max_delay = 0
        max_delay_pipeline = ""

        for rec in records:
            try:
                wm_dt = datetime.strptime(rec.current_watermark, rec.partition_format)
            except ValueError:
                wm_dt = now
            expected = self._expected_watermark(rec.scheduled_time, now)
            delay = self._compute_delay(wm_dt, expected)

            if delay > 120:
                severe += 1
            elif delay > 30:
                delayed += 1
            else:
                normal += 1

            if delay > max_delay:
                max_delay = delay
                max_delay_pipeline = rec.pipeline_id

        return {
            "total_pipelines": total,
            "normal": normal,
            "delayed": delayed,
            "severe": severe,
            "max_delay_minutes": max_delay,
            "max_delay_pipeline": max_delay_pipeline,
            "evaluation_time": now.isoformat(),
            "overall_health": "green" if severe == 0 and delayed < total * 0.2 else
                              "yellow" if severe < total * 0.1 else "red",
        }

    def compute_missing_partitions(
        self,
        record: WatermarkRecord,
        expected_watermark: str | None = None,
        confirmed_partitions: set[str] | None = None,
    ) -> list[str]:
        """依据有效进度计算实际缺失分区。

        枚举当前水位（不含）到期望水位（含）之间的全部日分区，
        再剔除进度回执中已确认有产物的分区，得到真正需要补偿的缺口。
        """
        now = datetime.now()
        expected_str = expected_watermark or self._expected_watermark(
            record.scheduled_time, now
        ).strftime(record.partition_format)
        candidates = self._enumerate_partitions(
            record.current_watermark, expected_str, record.partition_format
        )
        confirmed = confirmed_partitions or set()
        return [p for p in candidates if p not in confirmed]

    def should_trigger_backfill(
        self,
        record: WatermarkRecord,
        backfill_threshold_days: int = 3,
        confirmed_partitions: set[str] | None = None,
        expected_watermark: str | None = None,
    ) -> tuple[bool, str]:
        """判断是否需要触发回刷补偿。

        依据有效进度版本计算实际缺失分区：明确已到达的分区不再计入缺口，
        只有当实际缺失分区数达到 backfill_threshold_days 时才建议回刷，
        且建议范围与本次进度检查实际核验出的缺口一致。
        """
        try:
            wm_dt = datetime.strptime(
                record.current_watermark, record.partition_format
            )
        except ValueError:
            return False, "无法解析水位"

        now = datetime.now()
        expected_str = expected_watermark or self._expected_watermark(
            record.scheduled_time, now
        ).strftime(record.partition_format)

        missing = self.compute_missing_partitions(
            record,
            expected_watermark=expected_str,
            confirmed_partitions=confirmed_partitions,
        )

        if len(missing) >= backfill_threshold_days:
            return (
                True,
                f"实际缺失 {len(missing)} 个分区，建议回刷 "
                f"{missing[0]} ~ {missing[-1]}",
            )
        return False, ""

    # ---- 内部 ----

    @staticmethod
    def _enumerate_partitions(
        start: str, end: str, fmt: str
    ) -> list[str]:
        """枚举 start(不含) 到 end(含) 之间的所有分区值。"""
        try:
            start_dt = datetime.strptime(start, fmt)
            end_dt = datetime.strptime(end, fmt)
        except ValueError:
            return [end]

        partitions: list[str] = []
        current = start_dt + timedelta(days=1)
        while current <= end_dt:
            partitions.append(current.strftime(fmt))
            current += timedelta(days=1)
        return partitions

    @staticmethod
    def _expected_watermark(scheduled_time: datetime, now: datetime) -> datetime:
        """计算期望水位：当前时间向下取整到天（假设T+1调度）。"""
        # 简化模型：期望水位 = 昨天的日期
        return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def _compute_delay(actual: datetime, expected: datetime) -> int:
        """计算延迟分钟数（期望 vs 实际）。"""
        return max(0, int((expected - actual).total_seconds() / 60))