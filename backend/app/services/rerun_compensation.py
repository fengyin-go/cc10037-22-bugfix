"""重跑补偿领域服务：管理管线重跑任务、回刷范围编排和并行度控制。

上游：depend_scheduler 产出调度状态 → rerun_compensation 执行重跑
消费：backfill_tracker 消费回刷进度用于追踪和数据写入量统计

补偿口径与 app.engine.progress_compensation 中的进度校验回执保持一致：
可执行补偿范围只能由有效进度版本计算，明确到达的分区不得被延迟补算覆盖。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.engine.progress_compensation import progress_engine
from app.services.base import BaseModuleService
from app.store import store


class RerunCompensationService(BaseModuleService):
    MODULE = "rerun_compensation"
    ENTITY = "重跑任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务名称", "目标管线", "重跑范围", "开始分区", "结束分区", "并行度", "补偿原因"]
    REQUIRED_FIELDS = ["任务名称", "目标管线", "重跑范围"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "补偿失败", "已取消"]
    ACTION_RULES = {"开始回刷": "待执行", "完成补偿": "执行中", "标记失败": "执行中", "取消任务": "待执行"}
    NEGATIVE_ACTIONS = []

    def query_by_pipeline(self, pipeline_name: str) -> list[dict[str, Any]]:
        """按目标管线查询重跑任务。

        从 store 中筛选指定管线的重跑任务记录，
        返回任务列表供回刷进度面板消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if pipeline_name.casefold() in str(row.get("目标管线", "")).casefold():
                result.append(dict(row))
        return result

    def estimate_rerun_cost(self, task_name: str) -> dict[str, Any]:
        """估算重跑任务的计算资源消耗。

        根据重跑范围的分区跨度、并行度和数据量级估算计算时长和费用，
        产出预估数据供成本分析模块消费。
        """
        task = None
        for row in store.rows(self.MODULE):
            if row.get("任务名称") == task_name:
                task = row
                break
        if task is None:
            return {"error": f"重跑任务 {task_name} 不存在"}

        start_part = str(task.get("开始分区", "0"))
        end_part = str(task.get("结束分区", "0"))
        parallelism = int(task.get("并行度", "2"))
        try:
            start_h = int(start_part)
            end_h = int(end_part)
        except ValueError:
            start_h = 0
            end_h = 0

        partition_count = max(end_h - start_h, 1)
        avg_rows_per_part = 500000
        total_rows = partition_count * avg_rows_per_part
        estimated_seconds = total_rows / (parallelism * 10000)

        return {
            "task": task_name,
            "pipeline": task.get("目标管线"),
            "partition_count": partition_count,
            "estimated_rows": total_rows,
            "parallelism": parallelism,
            "estimated_seconds": round(estimated_seconds, 1),
            "estimated_minutes": round(estimated_seconds / 60, 1),
            "estimated_at": datetime.now().isoformat(),
        }

    def split_batch_range(self, task_name: str, batch_size: int = 3) -> list[dict[str, Any]]:
        """将重跑范围拆分为多个批次。

        根据并行度和开始/结束分区，将重跑任务拆分为可控的批次管道，
        支持分批提交和部分回滚。
        """
        task = None
        for row in store.rows(self.MODULE):
            if row.get("任务名称") == task_name:
                task = row
                break
        if task is None:
            return []

        start_part_str = str(task.get("开始分区", "0"))
        end_part_str = str(task.get("结束分区", "0"))
        try:
            start_h = int(start_part_str)
            end_h = int(end_part_str)
        except ValueError:
            start_h = 0
            end_h = 0

        total_parts = max(end_h - start_h, 1)
        batches: list[dict[str, Any]] = []
        for i in range(0, total_parts, batch_size):
            batch_start = start_h + i
            batch_end = min(start_h + i + batch_size, end_h)
            batches.append({
                "batch_no": len(batches) + 1,
                "start_part": str(batch_start),
                "end_part": str(batch_end),
                "partitions": batch_end - batch_start,
                "task": task_name,
                "status": "待执行",
            })

        return batches

    def compute_compensation_rate(self, pipeline_name: str) -> dict[str, Any]:
        """计算管线的补偿率。

        统计指定管线所有重跑任务的完成和失败情况，
        产出补偿率指标供运维质量评估。
        """
        tasks = self.query_by_pipeline(pipeline_name)
        total = len(tasks)
        completed = sum(1 for t in tasks if t.get("status") == "已完成")
        failed = sum(1 for t in tasks if t.get("status") == "补偿失败")
        in_progress = sum(1 for t in tasks if t.get("status") == "执行中")

        return {
            "pipeline": pipeline_name,
            "total_rerun_tasks": total,
            "completed": completed,
            "failed": failed,
            "in_progress": in_progress,
            "completion_rate_pct": round(completed / max(total, 1) * 100, 1),
            "failure_rate_pct": round(failed / max(total, 1) * 100, 1),
            "snapshot_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg

    # ---- 进度回执驱动的补偿领域能力 ----

    def executable_compensation_range(
        self, pipeline_name: str, receipt_version: int | None = None
    ) -> dict[str, Any]:
        """依据有效进度版本返回与某次检查相符的可执行补偿范围。"""
        return progress_engine.executable_compensation_range(
            pipeline_name, receipt_version=receipt_version
        )

    def create_compensation_batches(
        self,
        pipeline_name: str,
        *,
        receipt_version: int | None = None,
        partitions_per_batch: int = 1,
    ) -> list[dict[str, Any]]:
        """回刷补偿从可执行补偿范围创建批次。

        批次同步记录该次实际核验的进度回执版本和会写入的日期，
        并在 rerun_compensation 资源表中留痕。
        """
        batches = progress_engine.create_batches_from_range(
            pipeline_name,
            receipt_version=receipt_version,
            partitions_per_batch=partitions_per_batch,
        )
        entries: list[dict[str, Any]] = []
        for batch in batches:
            entry, _missing = self.create_entry({
                "任务名称": f"补偿批次 {batch.batch_no}",
                "目标管线": pipeline_name,
                "重跑范围": "日期范围",
                "开始分区": batch.write_partitions[0],
                "结束分区": batch.write_partitions[-1],
                "并行度": "1",
                "补偿原因": (
                    f"依据进度回执v{batch.basis_receipt_version}的实际缺失分区补偿，"
                    f"实际写入日期：{', '.join(batch.write_partitions)}"
                ),
            })
            if entry is not None:
                entry["批次编号"] = batch.batch_no
                entry["核验回执版本"] = batch.basis_receipt_version
                entry["实际写入日期"] = ",".join(batch.write_partitions)
                entries.append(entry)
        return entries

    def claim_missing_partition(
        self,
        pipeline_name: str,
        partition: str,
        *,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        """领取缺失分区补偿：领取依据与最新进度回执保持一致。

        本轮明确到达的日期不在缺失分区内，会被拒绝；
        并发领取同一日期只产生一张有效领取单。
        """
        claim, created = progress_engine.claim_partition(
            pipeline_name, partition, idempotency_key=idempotency_key
        )
        return {
            "created": created,
            "claim": claim.to_dict(),
            "message": "领取成功" if created else "该分区已有在途领取单，返回既有领取单",
        }

    def confirm_partition_write(
        self,
        pipeline_name: str,
        partition: str,
        *,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        """确认补偿写入：同一日期只确认一次有效写入。"""
        return progress_engine.confirm_partition_write(
            pipeline_name, partition, idempotency_key=idempotency_key
        )

    def reconcile_with_late_receipt(
        self,
        pipeline_name: str,
        confirmed_partitions: list[str],
        *,
        expected_partition: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        """迟到成功回执到达后重新对账。

        区分已被新进度抵销的日期与仍无产物的过往缺口，并解释旧建议为何缩短。
        """
        return progress_engine.register_late_receipt(
            pipeline_name,
            confirmed_partitions,
            expected_partition=expected_partition,
            note=note,
        )

    def compensation_snapshot(self, pipeline_name: str) -> dict[str, Any]:
        """返回该管线进度回执、补偿队列与实际写入日期的对账快照。"""
        return progress_engine.pipeline_snapshot(pipeline_name)