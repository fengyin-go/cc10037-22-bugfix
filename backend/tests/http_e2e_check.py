"""通过 HTTP 接口端到端复现题目场景（仅用标准库）。"""
from __future__ import annotations

import json
import threading
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8000/api"
PIPELINE = "DWS到ADS销售报表"
PIPELINE_ENC = urllib.parse.quote(PIPELINE)
TODAY = "2026-11-21"
BASELINE = "2026-11-15"
GAP = ["2026-11-16", "2026-11-17", "2026-11-18", "2026-11-19", "2026-11-20"]


def call(path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if payload is not None else "GET",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def show(title: str, obj: dict) -> None:
    print(f"\n===== {title} =====")
    print(json.dumps(obj, ensure_ascii=False, indent=2))


# 1) 以六天前的最后确认日期先启动补偿：首次检查，本日尚未到达
first = call("/business/watermark/check", {
    "pipeline_id": PIPELINE,
    "partition_field": "dt",
    "current_watermark": BASELINE,
    "new_watermark": BASELINE,
    "scheduled_time": "2026-11-21T09:00:00",
    "confirmed_partitions": [BASELINE],
    "baseline_partition": BASELINE,
    "expected_partition": TODAY,
})
show("步骤1 首次进度检查（基线11-15，本日未到）", {
    "receipt_version": first["receipt_version"],
    "status": first["new_status"],
    "missing_partitions": first["missing_partitions"],
    "backfill_plan": first["backfill_plan"],
})
assert first["missing_partitions"] == GAP + [TODAY]
assert first["backfill_plan"]["plan_start"] == "2026-11-16"

# 2) 从可执行补偿范围创建批次
batches = call("/business/compensation/batches", {
    "pipeline_id": PIPELINE, "partitions_per_batch": 2,
})
show("步骤2 从可执行范围创建补偿批次", {
    "basis_receipt_version": batches["basis_receipt_version"],
    "batches": batches["batches"],
    "rerun_entry_sample": batches["rerun_entries"][0],
})
assert len(batches["batches"]) == 3
assert batches["rerun_entries"][0]["核验回执版本"] == 1

# 3) 旧缺口补数进入队列
claim = call("/business/compensation/claims", {
    "pipeline_id": PIPELINE, "partition": "2026-11-16",
    "idempotency_key": "claim-16",
})
show("步骤3 领取旧缺口11-16", claim["claim"])
assert claim["created"] is True

# 4) 并发领取同一日期：只允许一张有效领取单
barrier = threading.Barrier(6)
outcomes: list[dict] = []


def concurrent_claim(i: int) -> None:
    barrier.wait()
    try:
        outcomes.append(call("/business/compensation/claims", {
            "pipeline_id": PIPELINE, "partition": "2026-11-17",
            "idempotency_key": f"c{i}",
        }))
    except Exception as exc:  # noqa: BLE001
        outcomes.append({"error": str(exc)})


threads = [threading.Thread(target=concurrent_claim, args=(i,)) for i in range(6)]
for t in threads:
    t.start()
for t in threads:
    t.join()
created = sum(1 for o in outcomes if o.get("created") is True)
show("步骤4 6线程并发领取11-17", {
    "threads": len(outcomes), "new_claims_created": created,
})
assert created == 1

# 重复写入确认只有一次有效
w1 = call("/business/compensation/write-confirmations", {
    "pipeline_id": PIPELINE, "partition": "2026-11-17",
    "idempotency_key": "w1",
})
w2 = call("/business/compensation/write-confirmations", {
    "pipeline_id": PIPELINE, "partition": "2026-11-17",
    "idempotency_key": "w2",
})
show("步骤4b 重复写入确认幂等", {"first": w1, "second": w2})
assert w1["write_effective"] is True
assert w2["write_effective"] is False

# 5) 迟到的本日成功回执到达
late = call("/business/progress/late-receipt", {
    "pipeline_id": PIPELINE,
    "confirmed_partitions": [BASELINE, TODAY],
    "expected_partition": TODAY,
})
show("步骤5 迟到本日成功回执对账", {
    "offset_partitions": late["offset_partitions"],
    "compensation_filled_partitions": late["compensation_filled_partitions"],
    "outstanding_gap_partitions": late["outstanding_gap_partitions"],
    "shortened_reason": late["shortened_reason"],
})
assert late["offset_partitions"] == [TODAY]
assert late["compensation_filled_partitions"] == ["2026-11-17"]
assert late["outstanding_gap_partitions"] == [
    "2026-11-16", "2026-11-18", "2026-11-19", "2026-11-20",
]

# 6) 明确到达的日期不得被延迟补算覆盖
try:
    call("/business/compensation/claims", {
        "pipeline_id": PIPELINE, "partition": TODAY,
    })
    raise AssertionError("本日分区应拒绝领取")
except urllib.error.HTTPError as exc:
    rejected = json.loads(exc.read().decode())
    show("步骤6 领取本日分区被拒绝", rejected)
    assert "缺失分区" in rejected["detail"]

# 7) 最终对账快照
snapshot = call(f"/business/compensation/{PIPELINE_ENC}/snapshot")
queued = snapshot["queued_write_partitions"]
show("步骤7 最终补偿队列对账", {
    "latest_receipt_version": snapshot["latest_receipt"]["receipt_version"],
    "executable_partitions": snapshot["executable_range"]["partitions"],
    "queued_write_partitions": queued,
    "confirmed_write_partitions": snapshot["confirmed_write_partitions"],
    "batch_write_dates": [
        b["write_partitions"] for b in snapshot["batches"]
    ],
})
assert TODAY not in snapshot["executable_range"]["partitions"]
assert TODAY not in [
    p for b in snapshot["batches"] for p in b["write_partitions"]
]
assert snapshot["confirmed_write_partitions"] == ["2026-11-17"]

# 8) 回归冒烟：原有资源接口与旧业务接口仍可用
health = urllib.request.urlopen(BASE + "/health").read().decode()
rerun_list = json.loads(urllib.request.urlopen(
    BASE + "/rerun_compensation?page=1&size=5"
).read().decode())
show("步骤8 回归冒烟", {
    "health": json.loads(health),
    "rerun_total": rerun_list["total"],
    "new_compensation_entries": [
        item["任务名称"] for item in rerun_list["items"]
        if item.get("核验回执版本")
    ][:3],
})
assert rerun_list["total"] >= 4

print("\n全部 HTTP 端到端断言通过 ✅")
