"""业务编排API：跨模块的ETL编排、质量门禁、回刷、Schema变更等操作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.engine.alert_matcher import AlertLevel, AlertRule
from app.engine.backfill_plan import BackfillEngine
from app.engine.progress_compensation import progress_engine
from app.engine.validation_executor import CheckLevel, CheckType, QualityRule
from app.engine.watermark_progress import WatermarkEngine, WatermarkRecord, WatermarkStatus
from app.orchestrator import ETLOrchestrator

router = APIRouter(prefix="/api/business", tags=["业务编排"])

_orchestrator = ETLOrchestrator()


def _guard(call):
    """把引擎/服务层的业务 ValueError 转成 409 结构化响应。"""
    try:
        return call()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


# ---- 管线编排 ----

@router.post("/pipelines/orchestrate")
def orchestrate_pipeline(body: dict[str, Any]) -> dict[str, Any]:
    """为管线执行完整的DAG拓扑验证和血缘登记。

    body:
      pipeline_id, pipeline_name, source_table, target_table,
      stages: [{"name": "extract", "nodes": [{"id": "n1", "name": "读取"}]}, ...]
    """
    try:
        return _orchestrator.create_pipeline_with_full_orchestration(
            pipeline_id=body["pipeline_id"],
            pipeline_name=body["pipeline_name"],
            source_table=body["source_table"],
            target_table=body["target_table"],
            stages=body.get("stages", []),
        )
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {e}")


# ---- 质量门禁 ----

@router.post("/quality/gate")
def run_quality_gate(body: dict[str, Any]) -> dict[str, Any]:
    """对管线数据执行质量门禁。

    body:
      pipeline_id, rules: [{"rule_id", "rule_name", "target_table", "check_field",
                             "check_type", "threshold", "level", "extra": {}}],
      sample_data: [dict]
    """
    pipeline_id = body["pipeline_id"]
    rules = [
        QualityRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            target_table=r.get("target_table", ""),
            check_field=r["check_field"],
            check_type=CheckType(r["check_type"]),
            threshold=float(r.get("threshold", 0.01)),
            level=CheckLevel(r.get("level", "WARN")),
            extra=r.get("extra", {}),
        )
        for r in body.get("rules", [])
    ]
    result = _orchestrator.run_quality_gate(
        pipeline_id, rules, body.get("sample_data", []),
    )
    return {
        "pipeline_id": result.pipeline_id,
        "overall_pass": result.overall_pass,
        "decision": result.decision,
        "failed_ratio": result.failed_ratio,
        "results": result.results,
        "triggered_alerts": [
            {"rule_id": a.rule_id, "rule_name": a.rule_name,
             "matched": a.matched, "level": a.level.value, "message": a.message}
            for a in result.triggered_alerts
        ],
    }


# ---- 水位与回刷 ----

@router.post("/watermark/check")
def check_watermark(body: dict[str, Any]) -> dict[str, Any]:
    """检查分区进度并判断是否需要回刷。

    body:
      pipeline_id, partition_field, current_watermark, new_watermark,
      scheduled_time (ISO), backfill_threshold (可选),
      confirmed_partitions (可选，本次实际核验到的分区),
      baseline_partition (可选，起算基线/最后确认日期),
      expected_partition (可选，期望到达分区，默认取 new_watermark)
    """
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=body["pipeline_id"],
        partition_field=body["partition_field"],
        current_watermark=body["current_watermark"],
        scheduled_time=datetime.fromisoformat(body["scheduled_time"]),
        status=WatermarkStatus(body.get("status", "正常")),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    return _guard(lambda: _orchestrator.check_watermark_and_backfill(
        record,
        body["new_watermark"],
        backfill_threshold=body.get("backfill_threshold", 3),
        confirmed_partitions=body.get("confirmed_partitions"),
        baseline_partition=body.get("baseline_partition"),
        expected_partition=body.get("expected_partition"),
    ))


# ---- 分区进度校验回执与补偿领取 ----

@router.post("/progress/receipts")
def register_progress_receipt(body: dict[str, Any]) -> dict[str, Any]:
    """登记一次分区进度检查，生成下一版本的进度校验回执。

    body:
      pipeline_id, confirmed_partitions, baseline_partition (首次必传),
      expected_partition (可选), status (可选), note (可选)
    """
    def _call() -> dict[str, Any]:
        receipt = progress_engine.register_receipt(
            pipeline_id=body["pipeline_id"],
            confirmed_partitions=list(body.get("confirmed_partitions", [])),
            baseline_partition=body.get("baseline_partition"),
            expected_partition=body.get("expected_partition"),
            check_status=body.get("status"),
            note=body.get("note", ""),
        )
        return {"ok": True, "receipt": receipt.to_dict()}

    return _guard(_call)


@router.post("/progress/late-receipt")
def register_late_receipt(body: dict[str, Any]) -> dict[str, Any]:
    """登记迟到的本日成功回执并对在途补偿对账。

    body:
      pipeline_id, confirmed_partitions, expected_partition (可选),
      status (可选), note (可选)
    """
    def _call() -> dict[str, Any]:
        result = progress_engine.register_late_receipt(
            pipeline_id=body["pipeline_id"],
            confirmed_partitions=list(body.get("confirmed_partitions", [])),
            expected_partition=body.get("expected_partition"),
            check_status=body.get("status"),
            note=body.get("note", ""),
        )
        return {"ok": True, **result}

    return _guard(_call)


@router.get("/progress/{pipeline_id}/missing")
def get_missing_partitions(
    pipeline_id: str, receipt_version: int | None = None
) -> dict[str, Any]:
    """依据有效进度版本计算管线的实际缺失分区。"""
    def _call() -> dict[str, Any]:
        missing = progress_engine.compute_missing_partitions(
            pipeline_id, receipt_version=receipt_version
        )
        receipt = (
            progress_engine.get_receipt(pipeline_id, receipt_version)
            if receipt_version is not None
            else progress_engine.latest_receipt(pipeline_id)
        )
        return {
            "pipeline_id": pipeline_id,
            "receipt_version": receipt.receipt_version if receipt else None,
            "missing_partitions": missing,
            "missing_count": len(missing),
        }

    return _guard(_call)


@router.get("/progress/{pipeline_id}/compensation-range")
def get_compensation_range(
    pipeline_id: str, receipt_version: int | None = None
) -> dict[str, Any]:
    """给出与指定进度检查相符的可执行补偿范围。"""
    return _guard(lambda: progress_engine.executable_compensation_range(
        pipeline_id, receipt_version=receipt_version
    ))


@router.post("/compensation/batches")
def create_compensation_batches(body: dict[str, Any]) -> dict[str, Any]:
    """从可执行补偿范围创建补偿批次，记录核验回执与实际写入日期。

    body:
      pipeline_id, receipt_version (可选), partitions_per_batch (可选)
    """
    from app.services import service_for

    def _call() -> dict[str, Any]:
        service = service_for("rerun_compensation")
        entries = service.create_compensation_batches(
            body["pipeline_id"],
            receipt_version=body.get("receipt_version"),
            partitions_per_batch=int(body.get("partitions_per_batch", 1)),
        )
        rng = progress_engine.executable_compensation_range(
            body["pipeline_id"], receipt_version=body.get("receipt_version")
        )
        batches = progress_engine.list_batches(body["pipeline_id"])
        new_batches = [
            batch.to_dict() for batch in batches
            if batch.basis_receipt_version == rng["basis_receipt_version"]
            and any(
                entry.get("批次编号") == batch.batch_no for entry in entries
            )
        ]
        return {
            "ok": True,
            "basis_receipt_version": rng["basis_receipt_version"],
            "compensation_range": rng,
            "batches": new_batches,
            "rerun_entries": entries,
        }

    return _guard(_call)


@router.post("/compensation/claims")
def claim_compensation(body: dict[str, Any]) -> dict[str, Any]:
    """领取一个缺失分区的补偿（并发幂等，同一日期只确认一次有效领取）。

    body: pipeline_id, partition, idempotency_key (可选)
    """
    from app.services import service_for

    def _call() -> dict[str, Any]:
        service = service_for("rerun_compensation")
        result = service.claim_missing_partition(
            body["pipeline_id"],
            body["partition"],
            idempotency_key=body.get("idempotency_key", ""),
        )
        return {"ok": True, **result}

    return _guard(_call)


@router.post("/compensation/write-confirmations")
def confirm_compensation_write(body: dict[str, Any]) -> dict[str, Any]:
    """确认补偿分区写入；重复确认幂等，被新进度抵销的日期拒绝写入。

    body: pipeline_id, partition, idempotency_key (可选)
    """
    from app.services import service_for

    def _call() -> dict[str, Any]:
        service = service_for("rerun_compensation")
        result = service.confirm_partition_write(
            body["pipeline_id"],
            body["partition"],
            idempotency_key=body.get("idempotency_key", ""),
        )
        return {"ok": True, **result}

    return _guard(_call)


@router.get("/compensation/{pipeline_id}/snapshot")
def compensation_snapshot(pipeline_id: str) -> dict[str, Any]:
    """查看管线进度校验与补偿队列的完整对账快照。"""
    return _guard(lambda: progress_engine.pipeline_snapshot(pipeline_id))


# ---- 回刷计划 ----

@router.post("/backfill/plan")
def generate_backfill_plan(body: dict[str, Any]) -> dict[str, Any]:
    """生成回刷补偿方案。

    body:
      pipeline_id, start_partition, end_partition, parallelism, reason
    """
    engine = BackfillEngine()
    plan = engine.plan(
        pipeline_id=body["pipeline_id"],
        start_partition=body["start_partition"],
        end_partition=body["end_partition"],
        parallelism=int(body.get("parallelism", 4)),
        reason=body.get("reason", ""),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    # 分配资源
    alloc = _orchestrator.allocate_for_pipeline(
        body["pipeline_id"],
        node_count=plan.total_partitions,
        estimated_memory_gb=plan.total_partitions * 0.5,
    )

    return {
        "plan": {
            "pipeline_id": plan.pipeline_id,
            "task_name": plan.task_name,
            "reason": plan.reason,
            "total_partitions": plan.total_partitions,
            "batch_count": len(plan.batches),
            "parallelism": plan.parallelism,
            "estimated_completion": plan.estimated_completion,
            "batches": [
                {
                    "batch_id": b.batch_id,
                    "partitions": b.range.partitions,
                    "count": b.range.count,
                }
                for b in plan.batches
            ],
        },
        "resource_allocation": alloc["allocation"],
    }


# ---- Schema变更评估 ----

@router.post("/schema/evaluate")
def evaluate_schema_change(body: dict[str, Any]) -> dict[str, Any]:
    """评估Schema变更的兼容性和影响。

    body:
      change_id, table_name,
      before_columns: [{"name", "data_type", "nullable", "is_partition"}],
      after_columns: [{"name", "data_type", "nullable", "is_partition"}],
      pipeline_deps: {table_name: [pipeline_ids]}
    """
    from app.engine.schema_evolution import SchemaColumn, SchemaDefinition

    before = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("before_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("before_columns", []) if c.get("is_partition")
        ],
    )
    after = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("after_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("after_columns", []) if c.get("is_partition")
        ],
    )
    result = _orchestrator.evaluate_schema_change(
        change_id=body.get("change_id", "auto"),
        before=before,
        after=after,
        pipeline_deps=body.get("pipeline_deps", {}),
    )
    return {
        "change_id": result.change.change_id,
        "table": result.change.target_table,
        "compatibility": result.change.compatibility.value,
        "changes": result.change.detail.get("changes", []),
        "affected_pipelines": result.change.affected_pipelines,
        "rollback_plan": result.change.rollback_plan,
        "migration_recommendation": result.migration_recommendation,
        "walk_pipelines": result.affected_pipelines,
    }


# ---- 血缘影响分析 ----

@router.get("/lineage/impact/{node_id}")
def analyze_lineage_impact(node_id: str) -> dict[str, Any]:
    """分析数据血缘影响：上游溯源 + 下游影响。"""
    result = _orchestrator.lineage.analyze_impact(node_id)
    return {
        "target_node": result.target_node,
        "upstream_count": len(result.upstream_nodes),
        "downstream_count": len(result.downstream_nodes),
        "affected_pipelines": result.affected_pipelines,
        "lineage_depth": result.lineage_depth,
        "has_cycle": result.has_cycle,
        "cycle_path": result.cycle_path,
    }


# ---- 资源健康 ----

@router.get("/resources/health")
def cluster_health() -> dict[str, Any]:
    """计算资源集群健康状态。"""
    return _orchestrator.balancer.cluster_health()


# ---- 告警批量评估 ----

@router.post("/alerts/evaluate")
def evaluate_alerts(body: dict[str, Any]) -> dict[str, Any]:
    """批量评估告警规则。

    body:
      rules: [{"rule_id", "rule_name", "monitor_target", "condition", "level"}],
      metrics: dict
    """
    rules = [
        AlertRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            monitor_target=r["monitor_target"],
            condition=r["condition"],
            level=AlertLevel(r.get("level", "WARN")),
            channels=r.get("channels", ["email"]),
        )
        for r in body.get("rules", [])
    ]
    matcher = _orchestrator.alert
    results = matcher.match_batch(rules, body.get("metrics", {}))

    return {
        "total_rules": len(rules),
        "triggered": sum(1 for r in results if r.matched),
        "results": [
            {
                "rule_id": r.rule_id,
                "rule_name": r.rule_name,
                "matched": r.matched,
                "level": r.level.value,
                "message": r.message,
            }
            for r in results
        ],
    }


# ---- DAG拓扑分析 ----

@router.post("/dag/analyze")
def analyze_dag(body: dict[str, Any]) -> dict[str, Any]:
    """分析DAG拓扑结构。

    body:
      nodes: [{"id", "name", "weight"}],
      edges: [{"from", "to"}]
    """
    from app.engine.dag_topology import DagNode, DagTopology

    topo = DagTopology()
    for n in body.get("nodes", []):
        topo.add_node(DagNode(
            node_id=n["id"], name=n.get("name", n["id"]),
            weight=float(n.get("weight", 1.0)),
        ))
    for e in body.get("edges", []):
        topo.add_edge(e["from"], e["to"])

    result = topo.solve()
    return {
        "node_count": topo.node_count,
        "has_cycle": result.has_cycle,
        "cycle_nodes": result.cycle_nodes,
        "topology_order": result.order,
        "critical_path": result.critical_path,
        "critical_length": result.critical_length,
        "node_depth": result.node_depth,
    }


# ---- 综合管线运行报告 ----

@router.get("/pipelines/{pipeline_id}/run-report")
def pipeline_run_report(pipeline_id: str) -> dict[str, Any]:
    """获取管线的综合运行报告。"""
    # 模拟生成运行报告
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=pipeline_id,
        partition_field="dt",
        current_watermark=datetime.now().strftime("%Y-%m-%d"),
        scheduled_time=datetime.now(),
        delay_minutes=0,
        status=WatermarkStatus.NORMAL,
    )

    # 模拟质量结果
    quality = _orchestrator.run_quality_gate(
        pipeline_id,
        [
            QualityRule(
                rule_id="r_default",
                rule_name="默认非空检查",
                target_table=f"dw.{pipeline_id}",
                check_field="id",
                check_type=CheckType.NOT_NULL,
                threshold=0.01,
            ),
        ],
        [{"id": "001"}, {"id": None}],
    )

    return _orchestrator.pipeline_run_report(pipeline_id, record, quality)