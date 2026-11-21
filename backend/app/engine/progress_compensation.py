"""分区进度校验与回刷补偿协同引擎。

解决"进度检查入口显示当天分区已到、状态正常，但补偿规划仍按六天前旧缺口切批"
的不一致问题。核心约定：

- 每次进度检查都生成一份带版本号的 **进度校验回执（ProgressReceipt）**，
  回执记录本次实际核验到的分区、依据的起算基线、期望分区以及真正缺失的分区；
- 补偿范围只能由"有效进度版本"（最新回执）计算，明确到达的分区不得再被
  延迟补算覆盖；
- 迟到的成功回执到达后，重新对账：已被新进度抵销的排队缺口标记为抵销，
  仍无产物的过往缺口保留在可执行补偿范围内，并给出旧建议缩短的原因；
- 同一缺失分区的领取与写入在进程内加锁并做幂等控制，并发领取只能确认
  一次有效写入。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

PARTITION_FORMAT = "%Y-%m-%d"


class ProgressStatus(str, Enum):
    """进度检查状态。"""

    NORMAL = "正常"        # 基线（含）到期望分区之间没有缺口
    DELAYED = "延迟"       # 存在缺失分区


class ClaimStatus(str, Enum):
    """补偿领取单状态。"""

    CLAIMED = "已领取"                # 已领取、尚未写入
    WRITE_CONFIRMED = "已确认写入"    # 已确认一次有效写入
    OFFSET = "已被新进度抵销"         # 迟到回执证明该分区已有产物，补偿作废


class BatchStatus(str, Enum):
    PENDING = "待执行"
    CLAIMED = "领取中"
    CONFIRMED = "已确认写入"
    OFFSET = "已被新进度抵销"


def _parse(partition: str) -> datetime:
    return datetime.strptime(partition, PARTITION_FORMAT)


def _fmt(dt: datetime) -> str:
    return dt.strftime(PARTITION_FORMAT)


def enumerate_partitions(start: str, end: str) -> list[str]:
    """枚举 start(含) 到 end(含) 之间的所有日分区，无法解析时退化为单分区。"""
    try:
        start_dt = _parse(start)
        end_dt = _parse(end)
    except ValueError:
        return [start]
    if end_dt < start_dt:
        return []
    partitions: list[str] = []
    current = start_dt
    while current <= end_dt:
        partitions.append(_fmt(current))
        current += timedelta(days=1)
    return partitions


@dataclass
class ProgressReceipt:
    """一次分区进度检查的校验回执（一个有效进度版本）。"""

    receipt_version: int
    pipeline_id: str
    baseline_partition: str          # 起算基线（最后确认日期）
    expected_partition: str          # 本次检查期望到达的分区
    verified_partitions: list[str]   # 本次实际核验到的分区（含历史已确认）
    missing_partitions: list[str]    # 依据本版本计算出的实际缺失分区
    offset_partitions: list[str]     # 被本次新进度抵销的缺口分区
    compensation_filled_partitions: list[str]  # 由补偿写入补齐、不再缺失的分区
    status: str
    checked_at: str
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_version": self.receipt_version,
            "pipeline_id": self.pipeline_id,
            "baseline_partition": self.baseline_partition,
            "expected_partition": self.expected_partition,
            "verified_partitions": list(self.verified_partitions),
            "missing_partitions": list(self.missing_partitions),
            "offset_partitions": list(self.offset_partitions),
            "compensation_filled_partitions": list(
                self.compensation_filled_partitions
            ),
            "status": self.status,
            "checked_at": self.checked_at,
            "note": self.note,
        }


@dataclass
class CompensationClaim:
    """一个缺失分区的补偿领取单。"""

    claim_id: int
    pipeline_id: str
    partition: str
    basis_receipt_version: int       # 领取依据的进度回执版本
    status: str
    claimed_at: str
    idempotency_key: str = ""
    offset_receipt_version: int | None = None
    confirmed_at: str | None = None
    write_idempotency_key: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "pipeline_id": self.pipeline_id,
            "partition": self.partition,
            "basis_receipt_version": self.basis_receipt_version,
            "status": self.status,
            "claimed_at": self.claimed_at,
            "idempotency_key": self.idempotency_key,
            "offset_receipt_version": self.offset_receipt_version,
            "confirmed_at": self.confirmed_at,
            "write_idempotency_key": self.write_idempotency_key,
        }


@dataclass
class CompensationBatch:
    """回刷补偿批次：记录所依据的核验回执与实际会写入的日期。"""

    batch_id: int
    batch_no: str
    pipeline_id: str
    basis_receipt_version: int
    write_partitions: list[str]
    status: str
    created_at: str
    claims: list[CompensationClaim] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "batch_no": self.batch_no,
            "pipeline_id": self.pipeline_id,
            "basis_receipt_version": self.basis_receipt_version,
            "write_partitions": list(self.write_partitions),
            "status": self.status,
            "created_at": self.created_at,
            "claims": [claim.to_dict() for claim in self.claims],
        }


@dataclass
class WriteConfirmation:
    """一次有效写入确认记录。"""

    write_id: int
    pipeline_id: str
    partition: str
    claim_id: int
    receipt_version: int
    idempotency_key: str
    written_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "write_id": self.write_id,
            "pipeline_id": self.pipeline_id,
            "partition": self.partition,
            "claim_id": self.claim_id,
            "receipt_version": self.receipt_version,
            "idempotency_key": self.idempotency_key,
            "written_at": self.written_at,
        }


class ProgressCompensationEngine:
    """进度校验回执 + 补偿范围计算 + 领取/写入幂等控制。

    用法:
        eng = ProgressCompensationEngine()
        receipt = eng.register_receipt(
            pipeline_id="p1", baseline_partition="2026-11-15",
            confirmed_partitions=["2026-11-15"],
        )
        batches = eng.create_batches_from_range("p1")
        claim, created = eng.claim_partition("p1", "2026-11-16")
        late = eng.register_late_receipt(
            "p1", confirmed_partitions=["2026-11-15", "2026-11-21"],
        )
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # pipeline -> 状态
        self._baselines: dict[str, str] = {}
        self._confirmed: dict[str, set[str]] = {}
        self._receipts: dict[str, list[ProgressReceipt]] = {}
        self._batches: dict[str, list[CompensationBatch]] = {}
        self._claims: dict[tuple[str, str], CompensationClaim] = {}
        self._writes: dict[str, list[WriteConfirmation]] = {}
        self._claim_seq = 0
        self._batch_seq = 0
        self._write_seq = 0
        # pipeline -> 已由补偿写入确认补齐的分区（区别于进度自然到达）
        self._compensation_filled: dict[str, set[str]] = {}

    # ---- 进度校验回执 ----

    def register_receipt(
        self,
        pipeline_id: str,
        confirmed_partitions: list[str],
        *,
        baseline_partition: str | None = None,
        expected_partition: str | None = None,
        check_status: str | None = None,
        note: str = "",
    ) -> ProgressReceipt:
        """登记一次分区进度检查，生成下一版本的校验回执。

        Args:
            pipeline_id: 管线标识
            confirmed_partitions: 本次检查实际核验到、确认已有产物的分区
            baseline_partition: 起算基线（最后确认日期），首次检查必传，
                后续不传则沿用上一版本基线
            expected_partition: 本次检查期望到达的分区，默认取当天
            check_status: 调用方依据延迟计算给出的状态；不传则按缺口自动判定
            note: 备注
        """
        with self._lock:
            now = datetime.now()
            expected = expected_partition or _fmt(now)
            baseline = baseline_partition or self._baselines.get(pipeline_id)
            if not baseline:
                raise ValueError("首次进度检查必须提供 baseline_partition（起算基线）")
            self._baselines[pipeline_id] = baseline

            confirmed_set = self._confirmed.setdefault(pipeline_id, set())
            new_arrivals = sorted(
                p for p in confirmed_partitions if p not in confirmed_set
            )
            confirmed_set.update(confirmed_partitions)
            # 基线本身视为已确认
            confirmed_set.add(baseline)

            previous_missing = set(
                self._receipts.get(pipeline_id, [])[-1].missing_partitions
                if self._receipts.get(pipeline_id) else []
            )
            missing = self._compute_missing_locked(pipeline_id, baseline, expected)
            resolved = previous_missing - set(missing)
            filled_by_compensation = self._compensation_filled.get(pipeline_id, set())
            # 被本次新进度（迟到回执）抵销的分区：不包含补偿写入补齐的部分
            offset = sorted(resolved - filled_by_compensation)
            compensation_filled = sorted(resolved & filled_by_compensation)

            status = check_status or (
                ProgressStatus.NORMAL.value if not missing
                else ProgressStatus.DELAYED.value
            )
            version = len(self._receipts.setdefault(pipeline_id, [])) + 1
            receipt = ProgressReceipt(
                receipt_version=version,
                pipeline_id=pipeline_id,
                baseline_partition=baseline,
                expected_partition=expected,
                verified_partitions=sorted(confirmed_set),
                missing_partitions=missing,
                offset_partitions=offset,
                compensation_filled_partitions=compensation_filled,
                status=status,
                checked_at=now.isoformat(),
                note=note,
            )
            self._receipts[pipeline_id].append(receipt)
            return receipt

    def register_late_receipt(
        self,
        pipeline_id: str,
        confirmed_partitions: list[str],
        *,
        expected_partition: str | None = None,
        check_status: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        """登记迟到的本日成功回执，并对在途补偿做对账。

        - 新进度明确到达的日期：排队中的领取单标记为"已被新进度抵销"，
          未领取的日期直接从可执行补偿范围剔除；
        - 仍无产物的过往缺口：保留为可执行补偿范围；
        - 同时返回旧建议缩短的原因。
        """
        with self._lock:
            prior_receipt = self.latest_receipt(pipeline_id)
            prior_missing = list(prior_receipt.missing_partitions) if prior_receipt else []
            receipt = self.register_receipt(
                pipeline_id,
                confirmed_partitions,
                expected_partition=expected_partition,
                check_status=check_status,
                note=note or "迟到成功回执到达，刷新进度校验回执",
            )

            offset_claims: list[CompensationClaim] = []
            active_claims: list[CompensationClaim] = []
            for partition in prior_missing:
                claim = self._claims.get((pipeline_id, partition))
                if claim is None or claim.status != ClaimStatus.CLAIMED.value:
                    continue
                if partition in receipt.offset_partitions:
                    claim.status = ClaimStatus.OFFSET.value
                    claim.offset_receipt_version = receipt.receipt_version
                    offset_claims.append(claim)
                else:
                    active_claims.append(claim)

            # 同步批次状态
            for batch in self._batches.get(pipeline_id, []):
                live = [
                    p for p in batch.write_partitions
                    if p in receipt.missing_partitions
                ]
                if not live:
                    batch.status = BatchStatus.OFFSET.value
                elif len(live) != len(batch.write_partitions):
                    batch.status = BatchStatus.CLAIMED.value
                batch.write_partitions = live

            shortened_reason = ""
            if receipt.offset_partitions:
                shortened_reason = (
                    f"回执v{receipt.receipt_version}确认分区 "
                    f"{', '.join(receipt.offset_partitions)} 已有产物，"
                    "旧补偿建议中这些日期被新进度抵销，可执行范围相应缩短"
                )

            return {
                "receipt": receipt.to_dict(),
                "offset_partitions": list(receipt.offset_partitions),
                "compensation_filled_partitions": list(
                    receipt.compensation_filled_partitions
                ),
                "outstanding_gap_partitions": list(receipt.missing_partitions),
                "offset_claims": [c.to_dict() for c in offset_claims],
                "active_claims": [c.to_dict() for c in active_claims],
                "shortened_reason": shortened_reason,
            }

    def latest_receipt(self, pipeline_id: str) -> ProgressReceipt | None:
        with self._lock:
            receipts = self._receipts.get(pipeline_id)
            return receipts[-1] if receipts else None

    def get_receipt(self, pipeline_id: str, version: int) -> ProgressReceipt | None:
        with self._lock:
            receipts = self._receipts.get(pipeline_id, [])
            if 1 <= version <= len(receipts):
                return receipts[version - 1]
            return None

    def list_receipts(self, pipeline_id: str) -> list[ProgressReceipt]:
        with self._lock:
            return list(self._receipts.get(pipeline_id, []))

    # ---- 缺失分区与可执行补偿范围 ----

    def _compute_missing_locked(
        self, pipeline_id: str, baseline: str, expected: str
    ) -> list[str]:
        """依据有效进度（已确认分区集合）计算基线之后到期望分区的实际缺口。"""
        confirmed = self._confirmed.get(pipeline_id, set())
        candidates = enumerate_partitions(
            _fmt(_parse(baseline) + timedelta(days=1)), expected
        )
        return [p for p in candidates if p not in confirmed]

    def compute_missing_partitions(
        self,
        pipeline_id: str,
        *,
        receipt_version: int | None = None,
    ) -> list[str]:
        """依据指定（默认最新）有效进度版本计算实际缺失分区。"""
        with self._lock:
            receipt = (
                self.get_receipt(pipeline_id, receipt_version)
                if receipt_version is not None else self.latest_receipt(pipeline_id)
            )
            if receipt is None:
                raise ValueError(f"管线 {pipeline_id} 尚无进度校验回执")
            return list(receipt.missing_partitions)

    def executable_compensation_range(
        self,
        pipeline_id: str,
        *,
        receipt_version: int | None = None,
    ) -> dict[str, Any]:
        """给出与指定进度检查相符的可执行补偿范围。

        只包含最新有效进度版本确认仍缺失、且尚未写入、未被抵销的分区。
        """
        with self._lock:
            receipt = (
                self.get_receipt(pipeline_id, receipt_version)
                if receipt_version is not None else self.latest_receipt(pipeline_id)
            )
            if receipt is None:
                raise ValueError(f"管线 {pipeline_id} 尚无进度校验回执")

            writable: list[str] = []
            excluded: list[dict[str, str]] = []
            for partition in receipt.missing_partitions:
                claim = self._claims.get((pipeline_id, partition))
                if claim and claim.status in (
                    ClaimStatus.WRITE_CONFIRMED.value, ClaimStatus.OFFSET.value
                ):
                    excluded.append({"partition": partition, "reason": claim.status})
                else:
                    writable.append(partition)

            return {
                "pipeline_id": pipeline_id,
                "basis_receipt_version": receipt.receipt_version,
                "baseline_partition": receipt.baseline_partition,
                "expected_partition": receipt.expected_partition,
                "start_partition": writable[0] if writable else None,
                "end_partition": writable[-1] if writable else None,
                "partitions": writable,
                "partition_count": len(writable),
                "excluded_partitions": excluded,
                "checked_at": receipt.checked_at,
            }

    # ---- 批次创建（回刷补偿从可执行补偿范围创建批次）----

    def create_batches_from_range(
        self,
        pipeline_id: str,
        *,
        receipt_version: int | None = None,
        partitions_per_batch: int = 1,
    ) -> list[CompensationBatch]:
        """从与进度检查相符的可执行补偿范围创建补偿批次。

        每个批次同步记录所依据的进度回执版本和实际会写入的日期。
        """
        with self._lock:
            rng = self.executable_compensation_range(
                pipeline_id, receipt_version=receipt_version
            )
            partitions = rng["partitions"]
            basis_version = rng["basis_receipt_version"]
            now_iso = datetime.now().isoformat()

            created: list[CompensationBatch] = []
            step = max(1, partitions_per_batch)
            for idx in range(0, len(partitions), step):
                chunk = partitions[idx:idx + step]
                self._batch_seq += 1
                batch = CompensationBatch(
                    batch_id=self._batch_seq,
                    batch_no=f"CB-{pipeline_id}-{self._batch_seq:04d}",
                    pipeline_id=pipeline_id,
                    basis_receipt_version=basis_version,
                    write_partitions=chunk,
                    status=BatchStatus.PENDING.value,
                    created_at=now_iso,
                )
                self._batches.setdefault(pipeline_id, []).append(batch)
                created.append(batch)
            return created

    def list_batches(self, pipeline_id: str) -> list[CompensationBatch]:
        with self._lock:
            return list(self._batches.get(pipeline_id, []))

    # ---- 领取与写入（幂等 + 并发控制）----

    def claim_partition(
        self,
        pipeline_id: str,
        partition: str,
        *,
        idempotency_key: str = "",
    ) -> tuple[CompensationClaim, bool]:
        """领取一个缺失分区的补偿。

        并发领取相同缺失日期只会产生一张有效领取单：
        - 已有在途领取单：原样返回（created=False）；
        - 分区已被新进度抵销或已写入：拒绝领取；
        - 领取依据必须与最新进度回执一致。
        返回 (领取单, 是否本次新建)。
        """
        with self._lock:
            receipt = self.latest_receipt(pipeline_id)
            if receipt is None:
                raise ValueError(f"管线 {pipeline_id} 尚无进度校验回执")

            existing = self._claims.get((pipeline_id, partition))
            if existing is not None:
                if existing.status == ClaimStatus.CLAIMED.value:
                    return existing, False
                if existing.status == ClaimStatus.WRITE_CONFIRMED.value:
                    raise ValueError(
                        f"分区 {partition} 已确认写入，不能重复领取"
                    )
                raise ValueError(
                    f"分区 {partition} 已被回执v{existing.offset_receipt_version}"
                    "的新进度抵销，不得再领取补偿"
                )

            if partition not in receipt.missing_partitions:
                raise ValueError(
                    f"分区 {partition} 不在回执v{receipt.receipt_version}"
                    "的实际缺失分区内，拒绝领取（避免覆盖已到达进度）"
                )

            self._claim_seq += 1
            claim = CompensationClaim(
                claim_id=self._claim_seq,
                pipeline_id=pipeline_id,
                partition=partition,
                basis_receipt_version=receipt.receipt_version,
                status=ClaimStatus.CLAIMED.value,
                claimed_at=datetime.now().isoformat(),
                idempotency_key=idempotency_key,
            )
            self._claims[(pipeline_id, partition)] = claim

            for batch in self._batches.get(pipeline_id, []):
                if partition in batch.write_partitions:
                    batch.claims.append(claim)
                    if batch.status == BatchStatus.PENDING.value:
                        batch.status = BatchStatus.CLAIMED.value
            return claim, True

    def confirm_partition_write(
        self,
        pipeline_id: str,
        partition: str,
        *,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        """确认补偿分区的一次有效写入。

        同一分区并发/重复确认只记一次有效写入；若该分区已被迟到的新进度
        抵销，则拒绝写入并把领取单标记为抵销。
        """
        with self._lock:
            claim = self._claims.get((pipeline_id, partition))
            if claim is None:
                raise ValueError(f"分区 {partition} 没有有效领取记录，无法确认写入")

            if claim.status == ClaimStatus.WRITE_CONFIRMED.value:
                prior = next(
                    (w for w in self._writes.get(pipeline_id, [])
                     if w.partition == partition),
                    None,
                )
                return {
                    "write_effective": False,
                    "reason": "该分区已确认过写入，重复确认按幂等忽略",
                    "write": prior.to_dict() if prior else None,
                }

            receipt = self.latest_receipt(pipeline_id)
            if (
                receipt is not None
                and partition not in receipt.missing_partitions
            ) or claim.status == ClaimStatus.OFFSET.value:
                claim.status = ClaimStatus.OFFSET.value
                claim.offset_receipt_version = (
                    receipt.receipt_version if receipt else claim.offset_receipt_version
                )
                return {
                    "write_effective": False,
                    "reason": (
                        f"分区 {partition} 已被回执v{claim.offset_receipt_version}"
                        "的新进度抵销，延迟补算不得覆盖该日期"
                    ),
                    "claim": claim.to_dict(),
                }

            claim.status = ClaimStatus.WRITE_CONFIRMED.value
            claim.confirmed_at = datetime.now().isoformat()
            claim.write_idempotency_key = idempotency_key
            self._confirmed.setdefault(pipeline_id, set()).add(partition)
            self._compensation_filled.setdefault(pipeline_id, set()).add(partition)

            self._write_seq += 1
            write = WriteConfirmation(
                write_id=self._write_seq,
                pipeline_id=pipeline_id,
                partition=partition,
                claim_id=claim.claim_id,
                receipt_version=claim.basis_receipt_version,
                idempotency_key=idempotency_key,
                written_at=claim.confirmed_at,
            )
            self._writes.setdefault(pipeline_id, []).append(write)

            for batch in self._batches.get(pipeline_id, []):
                if partition in batch.write_partitions and all(
                    self._claims.get((pipeline_id, p)) is not None
                    and self._claims[(pipeline_id, p)].status
                    == ClaimStatus.WRITE_CONFIRMED.value
                    for p in batch.write_partitions
                ):
                    batch.status = BatchStatus.CONFIRMED.value

            return {"write_effective": True, "write": write.to_dict()}

    def list_claims(self, pipeline_id: str) -> list[CompensationClaim]:
        with self._lock:
            return [
                claim for (pid, _), claim in sorted(
                    self._claims.items(), key=lambda item: item[1].claim_id
                )
                if pid == pipeline_id
            ]

    def list_writes(self, pipeline_id: str) -> list[WriteConfirmation]:
        with self._lock:
            return list(self._writes.get(pipeline_id, []))

    # ---- 总览快照 ----

    def pipeline_snapshot(self, pipeline_id: str) -> dict[str, Any]:
        """输出该管线进度校验与补偿队列的完整对账快照。"""
        with self._lock:
            receipt = self.latest_receipt(pipeline_id)
            claims = self.list_claims(pipeline_id)
            queued_write_partitions = [
                c.partition for c in claims
                if c.status == ClaimStatus.CLAIMED.value
            ]
            return {
                "pipeline_id": pipeline_id,
                "baseline_partition": self._baselines.get(pipeline_id),
                "confirmed_partitions": sorted(
                    self._confirmed.get(pipeline_id, set())
                ),
                "latest_receipt": receipt.to_dict() if receipt else None,
                "executable_range": (
                    self.executable_compensation_range(pipeline_id)
                    if receipt else None
                ),
                "claims": [c.to_dict() for c in claims],
                "queued_write_partitions": queued_write_partitions,
                "confirmed_write_partitions": [
                    w.partition for w in self._writes.get(pipeline_id, [])
                ],
                "batches": [
                    b.to_dict() for b in self._batches.get(pipeline_id, [])
                ],
            }

    def reset(self) -> None:
        """清空全部状态（主要供测试使用）。"""
        with self._lock:
            self._baselines.clear()
            self._confirmed.clear()
            self._receipts.clear()
            self._batches.clear()
            self._claims.clear()
            self._writes.clear()
            self._compensation_filled.clear()
            self._claim_seq = 0
            self._batch_seq = 0
            self._write_seq = 0


# 进程内单例：服务层、路由层与编排层共享同一份进度/补偿状态
progress_engine = ProgressCompensationEngine()
