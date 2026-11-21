"""分区进度检查与回刷补偿协同的端到端验证。

覆盖场景（对应题目）：
1. 进度检查入口确认当天分区已到、状态正常，但补偿面板仍从六天前切批的缺陷；
2. 依据有效进度版本计算实际缺失分区，给出与检查相符的可执行补偿范围；
3. 回刷补偿从可执行补偿范围创建批次，记录实际核验回执与会写入的日期；
4. 同一管线两轮处理：旧缺口在队列中，迟到的本日成功回执到达后，
   区分被新进度抵销的日期、仍无产物的过往缺口，并解释旧建议缩短原因；
5. 补偿领取依据与进度回执一致，明确到达的日期不得被延迟补算覆盖；
6. 并发领取同一缺失日期只能确认一次有效写入；
7. 先以六天前的最后确认日期启动补偿，再确认当天分区、刷新回执，
   核对补偿理由和队列实际写入日期。
"""
from __future__ import annotations

import sys
import threading
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine.progress_compensation import (  # noqa: E402
    ClaimStatus,
    ProgressStatus,
    progress_engine,
)
from app.engine.watermark_progress import (  # noqa: E402
    WatermarkEngine,
    WatermarkRecord,
    WatermarkStatus,
)
from app.orchestrator import ETLOrchestrator  # noqa: E402
from app.routers import business  # noqa: E402
from app.services import service_for  # noqa: E402

PIPELINE = "DWS到ADS销售报表"
TODAY = "2026-11-21"
BASELINE = "2026-11-15"          # 六天前的最后确认日期
GAP_DATES = [
    "2026-11-16", "2026-11-17", "2026-11-18", "2026-11-19", "2026-11-20",
]


class ProgressCompensationScenarioTest(unittest.TestCase):
    def setUp(self) -> None:
        progress_engine.reset()
        self.orchestrator = ETLOrchestrator()
        self.rerun_service = service_for("rerun_compensation")

    # ---- 步骤 1：以六天前的最后确认日期先启动补偿 ----

    def test_01_first_check_computes_real_missing_partitions(self) -> None:
        """首次检查：基线 11-15，本日分区尚未到达，缺失 11-16 ~ 11-21。"""
        record = WatermarkRecord(
            pipeline_id=PIPELINE,
            partition_field="dt",
            current_watermark=BASELINE,
            scheduled_time=datetime(2026, 11, 21, 9, 0),
            status=WatermarkStatus.DELAYED,
        )
        result = self.orchestrator.check_watermark_and_backfill(
            record,
            new_watermark=BASELINE,
            confirmed_partitions=[BASELINE],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )

        # 实际缺失分区必须包含本日（尚未到达）与五个过往缺口
        self.assertEqual(
            result["missing_partitions"], GAP_DATES + [TODAY]
        )
        self.assertEqual(result["receipt_version"], 1)
        self.assertNotEqual(
            result["receipt"]["status"], ProgressStatus.NORMAL.value
        )
        self.assertTrue(result["need_backfill"])
        # 回刷方案范围与本次检查缺口一致，而不是固定"昨天"或旧水位
        self.assertEqual(result["backfill_plan"]["plan_start"], "2026-11-16")
        self.assertEqual(result["backfill_plan"]["plan_end"], TODAY)
        self.assertEqual(
            result["backfill_plan"]["basis_receipt_version"], 1
        )

    def test_02_create_batches_from_executable_range(self) -> None:
        """回刷补偿从可执行补偿范围创建批次，并记录回执与写入日期。"""
        progress_engine.register_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )
        rng = progress_engine.executable_compensation_range(PIPELINE)
        self.assertEqual(rng["partitions"], GAP_DATES + [TODAY])
        self.assertEqual(rng["basis_receipt_version"], 1)

        entries = self.rerun_service.create_compensation_batches(
            PIPELINE, partitions_per_batch=2
        )
        # 6 个分区、每批 2 个 => 3 个批次
        self.assertEqual(len(entries), 3)
        first = entries[0]
        self.assertEqual(first["开始分区"], "2026-11-16")
        self.assertEqual(first["结束分区"], "2026-11-17")
        self.assertEqual(first["核验回执版本"], 1)
        self.assertEqual(first["实际写入日期"], "2026-11-16,2026-11-17")

        batches = progress_engine.list_batches(PIPELINE)
        self.assertEqual(
            sorted(d for b in batches for d in b.write_partitions),
            GAP_DATES + [TODAY],
        )
        self.assertTrue(all(b.basis_receipt_version == 1 for b in batches))

    def test_03_old_gap_claims_remain_in_queue(self) -> None:
        """旧缺口的补数领取后仍在队列中（等待写入确认）。"""
        progress_engine.register_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )
        progress_engine.create_batches_from_range(PIPELINE)

        claim, created = progress_engine.claim_partition(PIPELINE, "2026-11-16")
        self.assertTrue(created)
        self.assertEqual(claim.status, ClaimStatus.CLAIMED.value)
        self.assertEqual(claim.basis_receipt_version, 1)

        # 并发/重复领取同一日期：只返回既有领取单，不新建
        again, created_again = progress_engine.claim_partition(
            PIPELINE, "2026-11-16"
        )
        self.assertFalse(created_again)
        self.assertEqual(again.claim_id, claim.claim_id)

    def test_04_concurrent_claims_single_effective_write(self) -> None:
        """并发领取相同缺失日期，只能确认一次有效写入。"""
        progress_engine.register_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )
        progress_engine.create_batches_from_range(PIPELINE)

        results: list[tuple[str, bool]] = []
        barrier = threading.Barrier(8)

        def worker(worker_id: int) -> None:
            barrier.wait()
            try:
                _claim, created = progress_engine.claim_partition(
                    PIPELINE, "2026-11-17",
                    idempotency_key=f"w{worker_id}",
                )
                results.append(("claim", created))
            except ValueError:
                results.append(("claim-error", False))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        created_count = sum(1 for kind, ok in results if kind == "claim" and ok)
        self.assertEqual(created_count, 1)

        # 重复写入确认只有一次生效
        first = progress_engine.confirm_partition_write(
            PIPELINE, "2026-11-17", idempotency_key="write-1"
        )
        second = progress_engine.confirm_partition_write(
            PIPELINE, "2026-11-17", idempotency_key="write-2"
        )
        self.assertTrue(first["write_effective"])
        self.assertFalse(second["write_effective"])
        self.assertEqual(len(progress_engine.list_writes(PIPELINE)), 1)

        # 已写入的分区不能再领取
        with self.assertRaises(ValueError):
            progress_engine.claim_partition(PIPELINE, "2026-11-17")

    # ---- 步骤 2：迟到的本日成功回执到达，刷新进度校验回执 ----

    def test_05_late_receipt_offsets_today_but_keeps_old_gaps(self) -> None:
        progress_engine.register_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )
        progress_engine.create_batches_from_range(PIPELINE)
        # 旧缺口补数已在队列中
        queued_claim, _ = progress_engine.claim_partition(PIPELINE, "2026-11-16")
        self.assertEqual(queued_claim.status, ClaimStatus.CLAIMED.value)

        reconciliation = progress_engine.register_late_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE, TODAY],
            expected_partition=TODAY,
        )

        # 本日分区被新进度抵销
        self.assertEqual(reconciliation["offset_partitions"], [TODAY])
        # 过往缺口仍无产物，保留在可执行范围
        self.assertEqual(
            reconciliation["outstanding_gap_partitions"], GAP_DATES
        )
        self.assertIn(TODAY, reconciliation["shortened_reason"])

        receipt = reconciliation["receipt"]
        self.assertEqual(receipt["receipt_version"], 2)
        self.assertIn(TODAY, receipt["verified_partitions"])
        self.assertNotIn(TODAY, receipt["missing_partitions"])
        self.assertEqual(receipt["missing_partitions"], GAP_DATES)

        # 可执行补偿范围不再包含今天，且记录被排除的原因
        rng = progress_engine.executable_compensation_range(PIPELINE)
        self.assertEqual(rng["partitions"], GAP_DATES)
        self.assertEqual(rng["basis_receipt_version"], 2)

    def test_06_arrived_partition_cannot_be_claimed_or_overwritten(self) -> None:
        """本轮明确到达的日期不得被延迟补算覆盖。"""
        progress_engine.register_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )
        progress_engine.register_late_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE, TODAY],
            expected_partition=TODAY,
        )

        with self.assertRaises(ValueError):
            progress_engine.claim_partition(PIPELINE, TODAY)

        # 若在途领取单覆盖了今天，迟到回执后写入必须被拒绝并标记抵销
        progress_engine.reset()
        progress_engine.register_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )
        claim, _ = progress_engine.claim_partition(PIPELINE, TODAY)
        self.assertEqual(claim.status, ClaimStatus.CLAIMED.value)
        progress_engine.register_late_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE, TODAY],
            expected_partition=TODAY,
        )
        write_result = progress_engine.confirm_partition_write(PIPELINE, TODAY)
        self.assertFalse(write_result["write_effective"])
        self.assertIn("抵销", write_result["reason"])
        self.assertEqual(
            progress_engine._claims[(PIPELINE, TODAY)].status,
            ClaimStatus.OFFSET.value,
        )
        self.assertEqual(progress_engine.list_writes(PIPELINE), [])

    def test_07_claim_basis_matches_latest_receipt(self) -> None:
        """领取依据的回执版本必须与当前最新进度回执一致。"""
        r1 = progress_engine.register_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )
        claim, _ = progress_engine.claim_partition(PIPELINE, "2026-11-18")
        self.assertEqual(claim.basis_receipt_version, r1.receipt_version)

        progress_engine.register_late_receipt(
            PIPELINE,
            confirmed_partitions=[BASELINE, TODAY],
            expected_partition=TODAY,
        )
        # 仍缺失的日期再次领取时，依据刷新为回执 v2
        claim2, _ = progress_engine.claim_partition(PIPELINE, "2026-11-19")
        self.assertEqual(claim2.basis_receipt_version, 2)

    def test_08_full_workflow_via_router_and_snapshot(self) -> None:
        """走路由函数完成完整流程，并核对最终快照中的补偿理由与写入日期。"""
        # 第一次进度检查（路由入口）
        check_body = {
            "pipeline_id": PIPELINE,
            "partition_field": "dt",
            "current_watermark": BASELINE,
            "new_watermark": BASELINE,
            "scheduled_time": "2026-11-21T09:00:00",
            "confirmed_partitions": [BASELINE],
            "baseline_partition": BASELINE,
            "expected_partition": TODAY,
            "backfill_threshold": 3,
        }
        first = business.check_watermark(check_body)
        self.assertEqual(first["missing_partitions"], GAP_DATES + [TODAY])

        # 创建批次
        batch_resp = business.create_compensation_batches({
            "pipeline_id": PIPELINE,
            "partitions_per_batch": 3,
        })
        self.assertEqual(batch_resp["basis_receipt_version"], 1)
        self.assertEqual(len(batch_resp["batches"]), 2)
        self.assertEqual(
            batch_resp["compensation_range"]["partitions"],
            GAP_DATES + [TODAY],
        )

        # 先领取最早的旧缺口
        claim_resp = business.claim_compensation({
            "pipeline_id": PIPELINE, "partition": "2026-11-16",
        })
        self.assertTrue(claim_resp["created"])
        write_resp = business.confirm_compensation_write({
            "pipeline_id": PIPELINE, "partition": "2026-11-16",
        })
        self.assertTrue(write_resp["write_effective"])

        # 迟到的本日成功回执
        late = business.register_late_receipt({
            "pipeline_id": PIPELINE,
            "confirmed_partitions": [BASELINE, TODAY],
            "expected_partition": TODAY,
        })
        self.assertEqual(late["offset_partitions"], [TODAY])
        # 11-16 是补偿写入补齐的，不能算作新进度抵销
        self.assertEqual(
            late["compensation_filled_partitions"], ["2026-11-16"]
        )
        self.assertTrue(late["shortened_reason"])

        snapshot = business.compensation_snapshot(PIPELINE)
        # 队列实际会写入的日期：剩余四个旧缺口（16 已写入、21 被抵销）
        self.assertEqual(
            snapshot["executable_range"]["partitions"],
            ["2026-11-17", "2026-11-18", "2026-11-19", "2026-11-20"],
        )
        self.assertEqual(
            snapshot["confirmed_write_partitions"], ["2026-11-16"]
        )
        self.assertEqual(snapshot["latest_receipt"]["receipt_version"], 2)
        # 批次实际写入日期中不再保留今天
        queued_dates = [
            p for b in snapshot["batches"] for p in b["write_partitions"]
        ]
        self.assertNotIn(TODAY, queued_dates)


class WatermarkEngineRegressionTest(unittest.TestCase):
    def test_backfill_respects_confirmed_partitions(self) -> None:
        """引擎回归：已确认到达的分区不应再被判定为缺失。"""
        engine = WatermarkEngine()
        record = WatermarkRecord(
            pipeline_id="p",
            partition_field="dt",
            current_watermark=BASELINE,
            scheduled_time=datetime(2026, 11, 21, 9, 0),
        )
        # 旧逻辑会把 11-16~11-20（期望=昨天）整体算作缺口；
        # 新逻辑剔除已确认分区后只剩真实缺口
        missing = engine.compute_missing_partitions(
            record,
            expected_watermark=TODAY,
            confirmed_partitions={BASELINE, "2026-11-16", "2026-11-17"},
        )
        self.assertEqual(
            missing,
            ["2026-11-18", "2026-11-19", "2026-11-20", TODAY],
        )

        need, reason = engine.should_trigger_backfill(
            record,
            backfill_threshold_days=3,
            confirmed_partitions={
                BASELINE, "2026-11-16", "2026-11-17", "2026-11-18",
                "2026-11-19", "2026-11-20",
            },
            expected_watermark=TODAY,
        )
        self.assertFalse(need)
        self.assertEqual(reason, "")

    def test_normal_status_when_no_missing_partition(self) -> None:
        """当天分区已到且无缺口时，回执状态应为正常、无需补偿。"""
        progress_engine.reset()
        receipt = progress_engine.register_receipt(
            "p_normal",
            confirmed_partitions=[BASELINE, *GAP_DATES, TODAY],
            baseline_partition=BASELINE,
            expected_partition=TODAY,
        )
        self.assertEqual(receipt.missing_partitions, [])
        self.assertEqual(receipt.status, ProgressStatus.NORMAL.value)
        rng = progress_engine.executable_compensation_range("p_normal")
        self.assertEqual(rng["partitions"], [])
        self.assertIsNone(rng["start_partition"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
