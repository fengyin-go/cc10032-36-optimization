"""输出批次封口契约测试：重试替换、封口后拒绝追加、报告口径、检查点闸门与幂等。

覆盖要点：
  * 每次重试替换同一逻辑分区的失败状态，不新增落库记录；
  * 批次封口后不能再追加记录；
  * 完整性报告只统计最新成功结果和被放弃分区；
  * 检查点确认无活动失败项后才允许下一批复用同一版本；
  * 到达截止时间后可继续重试完整封口，也可记录放弃分区带缺口发布；
  * 同一 request_id 的并发重试只生效一次；
  * 事务失败回滚到原版本；
  * 同一份报告口径下保留 aggregate_sink / sink_output 两条持久化路径的不同版本。
"""
from __future__ import annotations

import threading

import pytest

from app.output_batches import (
    BATCH_SEALED_COMPLETE,
    BATCH_SEALED_GAP,
    OutputBatchLedger,
    batch_ledger,
    seed_demo_batches,
)
from app.services import service_for
from app.services.aggregate_sink import AggregateSinkService
from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.sink_output import SinkOutputService
from app.store import store

seed_demo_batches()


@pytest.fixture()
def ledger() -> OutputBatchLedger:
    return OutputBatchLedger()


def _rows_for(batch_id: str) -> list[dict]:
    return [r for r in store.rows("sink_output") if r.get("_batch_id") == batch_id]


def _open(ledger: OutputBatchLedger, partitions: list[str], version: str = "v1",
          source: str = "sink_output", task: str = "聚合落库样例1"):
    result = ledger.open_batch(
        task_name=task, sink_name="结果落地样例1", partitions=partitions,
        version=version, source=source,
    )
    assert result["ok"], result
    return result["batch"]["batch_id"]


class TestRetryReplacesFailures:
    def test_retry_does_not_grow_record_count(self, ledger):
        batch_id = _open(ledger, ["p0", "p1", "p2"])
        ledger.attempt(batch_id, forced_failures={"p1", "p2"})
        assert len(_rows_for(batch_id)) == 3

        for _ in range(3):
            ledger.attempt(batch_id, forced_failures={"p2"})
            ledger.attempt(batch_id, forced_failures=set())
            assert len(_rows_for(batch_id)) == 3, "重试不应追加新落库记录"

    def test_replaced_records_are_tracked(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, forced_failures={"p1"})
        result = ledger.attempt(batch_id, outcomes={"p1": True})
        assert result["replaced"] == 1
        confirm = ledger.confirm_batch(batch_id)
        assert confirm["replaced_records"][0]["partition"] == "p1"
        assert confirm["batch"]["replaced_count"] == 1

    def test_sealed_batch_rejects_appends(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, forced_failures={"p1"})
        ledger.seal(batch_id, abandon_failed=True)
        before = len(_rows_for(batch_id))
        blocked = ledger.attempt(batch_id)
        assert blocked["ok"] is False
        assert blocked["code"] == "batch_sealed"
        assert len(_rows_for(batch_id)) == before
        opened_again = ledger.open_batch(
            task_name="聚合落库样例1", sink_name="结果落地样例1",
            partitions=["p0", "p1"], version="v1", source="sink_output",
        )
        assert opened_again["batch"]["batch_id"] == batch_id
        assert opened_again["batch"]["sealed"] is True


class TestIntegrityReport:
    def test_report_counts_latest_success_only(self, ledger):
        batch_id = _open(ledger, ["p0", "p1", "p2"])
        ledger.attempt(batch_id, forced_failures={"p1", "p2"})
        ledger.attempt(batch_id, forced_failures=set())
        report = ledger.integrity_report(batch_id)["report"]
        assert report["expected"] == 3
        assert report["success"] == 3
        assert report["active_failures"] == 0
        assert report["replaced_count"] == 2

    def test_report_after_abandon(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, forced_failures={"p1"})
        sealed = ledger.seal(batch_id, abandon_failed=True)
        report = sealed["report"]
        assert report["status"] == BATCH_SEALED_GAP
        assert report["complete"] is False
        assert report["success"] == 1
        assert report["abandoned"] == 1
        assert report["abandoned_partitions"] == ["p1"]
        assert report["active_failures"] == 0
        assert sealed["receipt"]["complete"] is False

    def test_full_seal_is_complete(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, outcomes={"p0": True, "p1": True})
        sealed = ledger.seal(batch_id)
        assert sealed["batch"]["status"] == BATCH_SEALED_COMPLETE
        assert sealed["report"]["complete"] is True
        assert sealed["receipt"]["status_label"] == "批次完整"

    def test_cannot_seal_with_active_failures(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, forced_failures={"p1"})
        blocked = ledger.seal(batch_id)
        assert blocked["ok"] is False
        assert blocked["code"] == "active_failures"


class TestCheckpointGate:
    def test_gate_blocks_unsealed_batch(self, ledger):
        batch_id = _open(ledger, ["p0"])
        ledger.attempt(batch_id, forced_failures={"p0"})
        gate = ledger.checkpoint_gate(batch_id, "v1")
        assert gate["allowed"] is False
        assert any("尚未封口" in reason for reason in gate["reasons"])

    def test_gate_allows_next_batch_after_complete_seal(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"], version="v1")
        ledger.attempt(batch_id, outcomes={"p0": True, "p1": True})
        ledger.seal(batch_id)
        gate = ledger.checkpoint_gate(batch_id, "v1")
        assert gate["allowed"] is True
        assert gate["complete"] is True
        assert gate["report_version"] >= 1

    def test_gate_blocks_on_version_mismatch(self, ledger):
        batch_id = _open(ledger, ["p0"], version="v1")
        ledger.attempt(batch_id, outcomes={"p0": True})
        ledger.seal(batch_id)
        gate = ledger.checkpoint_gate(batch_id, "v2")
        assert gate["allowed"] is False
        assert any("版本不一致" in reason for reason in gate["reasons"])

    def test_checkpoint_service_persists_report_version(self):
        service: CheckpointMgrService = service_for("checkpoint_mgr")  # type: ignore[assignment]
        batch_id = _open(service_batch_ledger(service), ["p0"], version="v9")
        batch_ledger.attempt(batch_id, outcomes={"p0": True})
        batch_ledger.seal(batch_id)
        gate = service.verify_batch_gate(batch_id, "v9")
        assert gate["allowed"] is True
        assert gate["report_source_version"] == "v9"

        created = service.create_checkpoint_for_batch("检查点样例2", batch_id)
        assert created["ok"] is True
        assert created["gate"]["allowed"] is True


def service_batch_ledger(_service: CheckpointMgrService) -> OutputBatchLedger:
    return batch_ledger


class TestDeadlineBehaviour:
    def test_can_retry_after_deadline_until_complete(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, forced_failures={"p1"})
        # 把截止时间推到过去，模拟到达批次截止时间
        ledger._batches[batch_id]["deadline_at"] -= 10_000
        blocked = ledger.seal(batch_id)
        assert blocked["ok"] is False
        assert blocked["past_deadline"] is True
        retried = ledger.attempt(batch_id, outcomes={"p1": True})
        assert retried["ok"] is True
        sealed = ledger.seal(batch_id)
        assert sealed["ok"] is True
        assert sealed["report"]["complete"] is True

    def test_abandon_after_deadline_produces_gap_receipt(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, forced_failures={"p1"})
        ledger._batches[batch_id]["deadline_at"] -= 10_000
        sealed = ledger.seal(batch_id, abandon_failed=True)
        assert sealed["ok"] is True
        assert sealed["receipt"]["abandoned_partitions"] == ["p1"]
        assert sealed["report"]["status_label"] == "批次带缺口封口"


class TestIdempotencyAndRollback:
    def test_concurrent_retry_applies_once(self, ledger):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, forced_failures={"p1"}, request_id="first")
        after_first = ledger.confirm_batch(batch_id)["batch"]

        barrier = threading.Barrier(5)
        results: list[dict] = []

        def worker() -> None:
            barrier.wait()
            results.append(ledger.attempt(
                batch_id, outcomes={"p1": True}, request_id="retry-1",
            ))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        confirm = ledger.confirm_batch(batch_id)
        applied = [r for r in results if not r.get("idempotent_replay")]
        assert len(applied) == 1, "同一 request_id 的并发重试只应生效一次"
        assert confirm["batch"]["attempts"] == after_first["attempts"] + 1
        assert len(_rows_for(batch_id)) == 2

    def test_concurrent_attempts_keep_one_consistent_version(self, ledger):
        batch_id = _open(ledger, ["p0", "p1", "p2"])
        results: list[dict] = []

        def worker(index: int) -> None:
            results.append(ledger.attempt(batch_id, forced_failures=set()))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        rows = _rows_for(batch_id)
        assert len(rows) == 3, "并发写入每个分区只应保留一条记录"
        assert all(r["write_status"] == "success" for r in rows)
        report = ledger.integrity_report(batch_id)["report"]
        assert report["success"] == 3
        # 每个区只一条活动记录，并发重试不会导致页面总量增长
        assert len([r for r in store.rows("sink_output")
                    if r.get("_batch_id") == batch_id]) == 3

    def test_transaction_failure_rolls_back(self, ledger, monkeypatch):
        batch_id = _open(ledger, ["p0", "p1"])
        ledger.attempt(batch_id, outcomes={"p0": True, "p1": False})
        before = ledger.confirm_batch(batch_id)
        before_rows = len(_rows_for(batch_id))

        def explode(*_args, **_kwargs):
            raise RuntimeError("模拟事务中断")

        monkeypatch.setattr(ledger, "_find_record_row", explode)
        result = ledger.attempt(batch_id, outcomes={"p1": True})
        assert result["ok"] is False
        assert result["code"] == "transaction_failed"
        after = ledger.confirm_batch(batch_id)
        assert after["batch"]["attempts"] == before["batch"]["attempts"]
        assert after["batch"]["replaced_count"] == before["batch"]["replaced_count"]
        assert len(_rows_for(batch_id)) == before_rows


class TestTwoPersistencePaths:
    def test_report_keeps_source_versions(self):
        reports = batch_ledger.list_reports()
        sources = {(r["source"], r["source_version"]) for r in reports}
        assert ("aggregate_sink", "v2026.11.18") in sources
        assert ("sink_output", "v2026.11.19") in sources
        aggregate_report = next(
            r for r in reports if r["source"] == "aggregate_sink"
        )
        assert aggregate_report["complete"] is True
        swallowed = batch_ledger.integrity_report(
            aggregate_report["batch_id"], version="v2026.11.18",
        )
        assert swallowed["report"]["report_version"] == aggregate_report["report_version"]

    def test_aggregate_sink_flush_seals_when_all_success(self):
        service: AggregateSinkService = service_for("aggregate_sink")  # type: ignore[assignment]
        result = service.batch_flush_to_target(
            "聚合落库样例2",
            [{"partition": "part_00", "value": 1}, {"partition": "part_01", "value": 2}],
            version="v-path-agg",
        )
        assert result["ok"] is True
        assert result["written"] == 2
        assert result["report"]["source_version"] == "v-path-agg"
        assert result["report"]["source"] == "aggregate_sink"

    def test_sink_output_batch_write_is_partition_scoped(self):
        service: SinkOutputService = service_for("sink_output")  # type: ignore[assignment]
        results = [{"key": f"k{i}", "timestamp": 1_700_000_000 + i} for i in range(6)]
        written = service.batch_write_results("结果落地样例2", results)
        assert written["ok"] is True
        assert written["batch_id"]
        integrity = service.verify_output_integrity("结果落地样例2")
        assert integrity["total"] <= 6
        retried = service.retry_failed_writes("结果落地样例2", written["batch_id"])
        assert retried["ok"] is True
        integrity_after = service.verify_output_integrity("结果落地样例2")
        assert integrity_after["total"] == integrity["total"], "重试不应让页面总量增长"
