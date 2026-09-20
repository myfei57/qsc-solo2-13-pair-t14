"""事务发件箱核心：入箱幂等、状态机、退避、死信、对账。"""

from __future__ import annotations

import json
import unittest

from flashsmelter.audit import AUDIT_STREAM, AuditLog
from flashsmelter.config import Settings
from flashsmelter.ns import Namespace
from flashsmelter.outbox import (
    DEAD,
    QUEUED,
    SENDING,
    SENT,
    DeliveryResult,
    KeyEventSelector,
    Outbox,
)
from flashsmelter.runtime import ManualClock
from flashsmelter.store import DurableStore

from tests.helpers import make_root


class RecordingTransport:
    """按脚本返回结果的假传输；对同一 event_id 只承认一次，模拟对端幂等。"""

    def __init__(self, script: list[DeliveryResult] | None = None) -> None:
        self.script = list(script or [])
        self.received: list[dict] = []
        self.acknowledged: set[str] = set()

    def send(self, envelope) -> DeliveryResult:
        event_id = envelope["event_id"]
        if event_id in self.acknowledged:
            return DeliveryResult("ok", status_code=200, receiver_receipt={"dedup": True})
        result = self.script.pop(0) if self.script else DeliveryResult("ok", status_code=200)
        self.received.append(dict(envelope))
        if result.ok:
            self.acknowledged.add(event_id)
        return result


class OutboxCoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.store = DurableStore(make_root("flashsmelter-outbox-"), clock=self.clock)
        self.namespace = Namespace.parse("smelter/line1")
        self.audit = AuditLog(self.store, self.namespace, self.clock)
        self.outbox = Outbox(
            self.store,
            KeyEventSelector(None),
            self.namespace,
            self.clock,
            event_decoder=self.audit.event_from_entry,
            max_attempts=3,
        )

    def _audit(self, action: str, *, outcome: str = "ok", target: str = "furnace"):
        return self.audit.record(
            actor="tester",
            action=action,
            target=target,
            outcome=outcome,
            correlation_id="corr-1",
            details={"heat_id": "H-1"},
        )

    # ------------------------------------------------------------- 选择器
    def test_selector_default_catalog_covers_key_moments(self) -> None:
        selector = KeyEventSelector(None)
        for action in ("furnace.feed", "furnace.tap", "furnace.latch", "burner.trip"):
            self.assertTrue(selector.is_key_event({"action": action, "target": "", "outcome": "ok"}), action)
        self.assertFalse(selector.is_key_event({"action": "settler.update", "target": "", "outcome": "ok"}))
        # latch 的拒绝尝试也要外发，但 feed 的拒绝不在目录内
        self.assertTrue(selector.is_key_event({"action": "furnace.latch", "target": "", "outcome": "rejected"}))
        self.assertFalse(selector.is_key_event({"action": "furnace.feed", "target": "", "outcome": "rejected"}))

    def test_selector_custom_patterns(self) -> None:
        selector = KeyEventSelector(("oxygen.*", "waste.update:failed"))
        self.assertTrue(selector.is_key_event({"action": "oxygen.ramp", "target": "", "outcome": "ok"}))
        self.assertFalse(selector.is_key_event({"action": "furnace.feed", "target": "", "outcome": "ok"}))
        self.assertTrue(selector.is_key_event({"action": "waste.update", "target": "", "outcome": "failed"}))
        self.assertFalse(selector.is_key_event({"action": "waste.update", "target": "", "outcome": "ok"}))

    def test_invalid_selector_pattern_rejected_at_config(self) -> None:
        from flashsmelter.errors import ValidationError as FsValidationError

        with self.assertRaises(FsValidationError):
            Settings.from_env({"FLASHSMELTER_OUTBOX_KEY_EVENTS": "no-dot"}, root=make_root())

    # ------------------------------------------------------------- 入箱
    def test_key_event_enqueued_with_audit_anchor(self) -> None:
        event = self._audit("furnace.feed")
        entry = self.outbox.enqueue_audit_entry(
            self.store.read_stream(AUDIT_STREAM, limit=1)[-1]
        )
        self.assertIsNotNone(entry)
        envelope = entry.envelope
        self.assertEqual(event.seq, envelope["audit_seq"])
        self.assertEqual(event.checksum, envelope["audit_checksum"])
        self.assertEqual("furnace.feed", envelope["event"]["action"])
        self.assertEqual("smelter/line1", envelope["namespace"])
        self.assertTrue(envelope["event_id"])
        self.assertTrue(envelope["payload_checksum"])

    def test_non_key_event_not_enqueued(self) -> None:
        self._audit("settler.update")
        result = self.outbox.enqueue_audit_entry(self.store.read_stream(AUDIT_STREAM, limit=1)[-1])
        self.assertIsNone(result)
        self.assertEqual(0, self.outbox.stats()["total"])

    def test_enqueue_is_idempotent_for_same_audit_seq(self) -> None:
        self._audit("furnace.feed")
        journal_entry = self.store.read_stream(AUDIT_STREAM, limit=1)[-1]
        first = self.outbox.enqueue_audit_entry(journal_entry)
        second = self.outbox.enqueue_audit_entry(journal_entry)
        self.assertEqual(first.seq, second.seq)
        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(1, self.outbox.stats()["total"])

    def test_incremental_ingest_backfills_missed_enqueue(self) -> None:
        # 不经即时入箱，只落审计：模拟通知钩子失败/旧进程没装发件箱。
        self._audit("furnace.feed")
        self._audit("furnace.tap")
        self._audit("settler.update")  # 非关键，跳过
        ingested = self.outbox.ingest_audit(AUDIT_STREAM)
        self.assertEqual(2, ingested)
        # 再扫一轮不重复入箱
        self.assertEqual(0, self.outbox.ingest_audit(AUDIT_STREAM))
        self.clock.advance(10)
        self._audit("matte.tap")
        self.assertEqual(1, self.outbox.ingest_audit(AUDIT_STREAM))
        self.assertEqual([1, 2, 4], [record.audit_seq for record in self.outbox.pending()])

    # ------------------------------------------------------------- 状态机
    def test_due_respects_audit_order(self) -> None:
        for action in ("furnace.feed", "furnace.tap", "furnace.stop"):
            self._audit(action)
        self.outbox.ingest_audit(AUDIT_STREAM)
        due = self.outbox.due()
        self.assertEqual([1, 2, 3], [record.audit_seq for _, record in due])

    def test_claim_and_mark_sent_persists_receipt(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        entry, record = self.outbox.due()[0]
        claimed = self.outbox.claim(record)
        self.assertEqual(SENDING, claimed.state)
        result = DeliveryResult("ok", status_code=200, receiver_receipt={"id": "rcp-1"})
        sent = self.outbox.mark_sent(claimed, result)
        self.assertEqual(SENT, sent.state)
        self.assertEqual(1, sent.attempts)
        self.assertEqual({"id": "rcp-1"}, dict(sent.receiver_receipt))
        self.assertTrue(sent.sent_at)
        self.assertEqual([], self.outbox.pending())
        stats = self.outbox.stats()
        self.assertEqual(1, stats["sent"])
        self.assertEqual(0, stats["queued"])

    def test_retry_backoff_blocks_due_until_expired(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        _, record = self.outbox.due()[0]
        claimed = self.outbox.claim(record)
        retried = self.outbox.mark_retry(
            claimed, DeliveryResult("transient", status_code=503, reason="http-503"), backoff_seconds=10
        )
        self.assertEqual(QUEUED, retried.state)
        self.assertEqual(1, retried.attempts)
        self.assertEqual([], self.outbox.due())
        self.clock.advance(10)
        due = self.outbox.due()
        self.assertEqual(1, len(due))
        self.assertEqual("http-503", due[0][1].last_error)

    def test_ordering_never_jumps_over_undelivered_oldest(self) -> None:
        for action in ("furnace.feed", "furnace.tap"):
            self._audit(action)
        self.outbox.ingest_audit(AUDIT_STREAM)
        _, first = self.outbox.due()[0]
        self.outbox.mark_retry(
            self.outbox.claim(first), DeliveryResult("transient", reason="offline"), backoff_seconds=100
        )
        # 退避未到期：什么都取不到，不会越过第一条去送第二条
        self.assertEqual([], self.outbox.due())
        self.clock.advance(100)
        due = self.outbox.due()
        self.assertEqual([1, 2], [record.audit_seq for _, record in due])

    def test_max_attempts_moves_event_to_dead(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        record = self.outbox.due()[0][1]
        for _ in range(3):
            record = self.outbox.mark_retry(
                self.outbox.claim(record),
                DeliveryResult("transient", reason="offline"),
                backoff_seconds=0,
            )
        self.assertEqual(DEAD, record.state)
        self.assertEqual(3, record.attempts)
        self.assertEqual([1], [item.audit_seq for item in self.outbox.dead()])
        # 死信不再被 due 捞起
        self.assertEqual([], self.outbox.due())

    def test_permanent_failure_marks_dead_immediately(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        _, record = self.outbox.due()[0]
        dead = self.outbox.mark_dead(
            self.outbox.claim(record), DeliveryResult("permanent", status_code=400, reason="bad-payload")
        )
        self.assertEqual(DEAD, dead.state)
        self.assertEqual("bad-payload", dead.last_error)

    def test_revive_dead_letter_requeues(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        _, record = self.outbox.due()[0]
        dead = self.outbox.mark_dead(self.outbox.claim(record), DeliveryResult("permanent", reason="x"))
        revived = self.outbox.revive(dead.audit_seq)
        self.assertEqual(QUEUED, revived.state)
        self.assertEqual(1, len(self.outbox.due()))

    def test_sending_interrupted_by_crash_is_redelivered(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        _, record = self.outbox.due()[0]
        self.outbox.claim(record)  # 进程在此刻崩溃：状态停在 sending
        reopened = Outbox(
            self.store,
            KeyEventSelector(None),
            self.namespace,
            self.clock,
            event_decoder=self.audit.event_from_entry,
            max_attempts=3,
        )
        due = reopened.due()
        self.assertEqual(1, len(due))
        self.assertEqual(SENDING, due[0][1].state)

    # ------------------------------------------------------------- 对账
    def test_reconcile_clean(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        _, record = self.outbox.due()[0]
        self.outbox.mark_sent(self.outbox.claim(record), DeliveryResult("ok", status_code=200))
        report = self.outbox.reconcile(AUDIT_STREAM)
        self.assertTrue(report["ok"])
        self.assertEqual(1, report["key_audit_events"])
        self.assertEqual(1, report["outbox_events"])
        self.assertEqual([], report["missing"])
        self.assertEqual([], report["undelivered"])

    def test_reconcile_flags_missing_after_manual_cursor_skip(self) -> None:
        self._audit("furnace.feed")
        report = self.outbox.reconcile(AUDIT_STREAM)
        self.assertFalse(report["ok"])
        self.assertEqual([1], report["missing"])

    def test_reconcile_detects_tampered_envelope_payload(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        path = self.store.journal_root / "outbox" / "events.jsonl"
        lines = path.read_text(encoding="utf-8").splitlines()
        entry = json.loads(lines[0])
        entry["payload"]["event"]["details"]["heat_id"] = "H-TAMPERED"
        path.write_text(json.dumps(entry, ensure_ascii=False) + "\n", encoding="utf-8")
        # 流水行 checksum 也会因此失效：对账直接读原始 payload，篡改必须被发现
        report = self.outbox.reconcile(AUDIT_STREAM)
        self.assertFalse(report["ok"])
        self.assertTrue(report["tampered"])

    def test_reconcile_lists_undelivered(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        report = self.outbox.reconcile(AUDIT_STREAM)
        self.assertEqual([1], report["undelivered"])

    def test_state_survives_reopen(self) -> None:
        self._audit("furnace.feed")
        self.outbox.ingest_audit(AUDIT_STREAM)
        _, record = self.outbox.due()[0]
        self.outbox.mark_sent(self.outbox.claim(record), DeliveryResult("ok", status_code=200))
        reopened = Outbox(
            self.store,
            KeyEventSelector(None),
            self.namespace,
            self.clock,
            event_decoder=self.audit.event_from_entry,
            max_attempts=3,
        )
        self.assertEqual(1, reopened.stats()["sent"])
        self.assertEqual(0, reopened.ingest_audit(AUDIT_STREAM))


if __name__ == "__main__":
    unittest.main()
