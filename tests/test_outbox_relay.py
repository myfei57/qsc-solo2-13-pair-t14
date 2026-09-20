"""外发中继：断网垫存、恢复续送、顺序投递、幂等头与退避。"""

from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from flashsmelter.audit import AUDIT_STREAM, AuditLog
from flashsmelter.ns import Namespace
from flashsmelter.outbox import DeliveryResult, HttpTransport, KeyEventSelector, Outbox, Relay
from flashsmelter.runtime import ManualClock
from flashsmelter.store import DurableStore

from tests.helpers import make_root


class ScriptedTransport:
    def __init__(self, results: list[DeliveryResult]) -> None:
        self.results = list(results)
        self.calls: list[str] = []

    def send(self, envelope) -> DeliveryResult:
        self.calls.append(str(envelope["event"]["action"]))
        return self.results.pop(0)


class RelayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.store = DurableStore(make_root("flashsmelter-relay-"), clock=self.clock)
        self.namespace = Namespace.parse("smelter/line1")
        self.audit = AuditLog(self.store, self.namespace, self.clock)
        self.outbox = Outbox(
            self.store,
            KeyEventSelector(None),
            self.namespace,
            self.clock,
            event_decoder=self.audit.event_from_entry,
            max_attempts=4,
        )
        for action in ("furnace.feed", "furnace.tap", "furnace.stop"):
            self.audit.record(
                actor="tester",
                action=action,
                target="furnace",
                outcome="ok",
                correlation_id="c1",
                details={},
            )
        self.outbox.ingest_audit(AUDIT_STREAM)

    def _relay(self, transport) -> Relay:
        return Relay(
            self.outbox,
            transport,
            clock=self.clock,
            poll_interval_seconds=0.01,
            backoff_seconds=10.0,
            backoff_max_seconds=100.0,
        )

    def test_offline_buffers_then_resumes_in_order(self) -> None:
        transport = ScriptedTransport(
            [
                DeliveryResult("transient", status_code=503, reason="offline"),
                DeliveryResult("ok", status_code=200),
                DeliveryResult("ok", status_code=200),
                DeliveryResult("ok", status_code=200),
            ]
        )
        relay = self._relay(transport)
        first = relay.run_once()
        self.assertEqual({"sent": 0, "retried": 1, "dead": 0, "attempts": 1, "ingested": 0}, first)
        self.assertEqual(3, self.outbox.stats()["queued"])
        # 退避未到：什么都不送
        self.assertEqual({"attempts": 0, "ingested": 0, "sent": 0, "retried": 0, "dead": 0}, relay.run_once())
        self.clock.advance(10)
        recovered = relay.run_once()
        self.assertEqual(3, recovered["sent"])
        self.assertEqual(["furnace.feed", "furnace.feed", "furnace.tap", "furnace.stop"], transport.calls)
        self.assertEqual(3, self.outbox.stats()["sent"])

    def test_transport_exception_treated_as_transient(self) -> None:
        class Exploding:
            def send(self, envelope):
                raise RuntimeError("socket gone")

        relay = self._relay(Exploding())
        counts = relay.run_once()
        self.assertEqual(1, counts["retried"])
        self.assertEqual(1, self.outbox.pending()[0].attempts)

    def test_no_transport_only_ingests(self) -> None:
        relay = Relay(self.outbox, None, clock=self.clock)
        counts = relay.run_once()
        self.assertEqual(0, counts["attempts"])
        self.assertEqual(3, self.outbox.stats()["queued"])

    def test_permanent_failure_dead_but_others_continue(self) -> None:
        transport = ScriptedTransport(
            [
                DeliveryResult("permanent", status_code=400, reason="bad"),
                DeliveryResult("ok", status_code=200),
                DeliveryResult("ok", status_code=200),
                DeliveryResult("ok", status_code=200),
            ]
        )
        relay = self._relay(transport)
        counts = relay.run_once()
        self.assertEqual(1, counts["dead"])
        self.assertEqual(2, counts["sent"])
        self.assertEqual(1, self.outbox.stats()["dead"])
        self.assertEqual(2, self.outbox.stats()["sent"])

    def test_exponential_backoff_caps_at_max(self) -> None:
        relay = self._relay(ScriptedTransport([]))
        self.assertEqual(10.0, relay._backoff_for(1))
        self.assertEqual(20.0, relay._backoff_for(2))
        self.assertEqual(80.0, relay._backoff_for(4))
        self.assertEqual(100.0, relay._backoff_for(5))

    def test_background_thread_starts_and_stops(self) -> None:
        relay = self._relay(ScriptedTransport([DeliveryResult("ok", status_code=200)] * 3))
        relay.start()
        try:
            self.assertTrue(relay.running)
            import time

            for _ in range(50):
                if self.outbox.stats()["sent"] == 3:
                    break
                time.sleep(0.02)
            self.assertEqual(3, self.outbox.stats()["sent"])
        finally:
            relay.stop()
        self.assertFalse(relay.running)


# ------------------------------------------------------------- 真实 HTTP 端到端
class _ReceiverHandler(BaseHTTPRequestHandler):
    received: list[dict] = []
    fail_times = 0
    attempts_by_id: dict[str, int] = {}
    idempotency_seen: set[str] = set()
    lock = threading.Lock()

    def log_message(self, *args):  # noqa: A002
        pass

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        event_id = self.headers.get("Idempotency-Key", "")
        with self.lock:
            duplicate = event_id in self.idempotency_seen
            self.attempts_by_id[event_id] = self.attempts_by_id.get(event_id, 0) + 1
            attempt = self.attempts_by_id[event_id]
            if attempt <= type(self).fail_times:
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.idempotency_seen.add(event_id)  # 只有成功承认才占幂等位
            self.received.append({"body": body, "headers": dict(self.headers), "duplicate": duplicate})
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        payload = json.dumps({"accepted": True, "event_id": event_id}).encode("utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    @classmethod
    def reset(cls, fail_times: int = 0) -> None:
        cls.received = []
        cls.fail_times = fail_times
        cls.attempts_by_id = {}
        cls.idempotency_seen = set()


class HttpTransportEndToEndTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _ReceiverHandler)
        cls.host, cls.port = cls.server.server_address[:2]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        _ReceiverHandler.reset(fail_times=2)
        self.clock = ManualClock()
        self.store = DurableStore(make_root("flashsmelter-http-"), clock=self.clock)
        self.namespace = Namespace.parse("smelter/line1")
        self.audit = AuditLog(self.store, self.namespace, self.clock)
        self.outbox = Outbox(
            self.store,
            KeyEventSelector(("furnace.*",)),
            self.namespace,
            self.clock,
            event_decoder=self.audit.event_from_entry,
            max_attempts=5,
        )
        self.transport = HttpTransport(f"http://{self.host}:{self.port}/events", timeout_seconds=5, clock=self.clock)
        self.relay = Relay(
            self.outbox,
            self.transport,
            clock=self.clock,
            backoff_seconds=1.0,
            backoff_max_seconds=4.0,
        )
        self.audit.record(
            actor="tester", action="furnace.feed", target="furnace", outcome="ok",
            correlation_id="c1", details={"heat_id": "H-9"},
        )
        self.outbox.ingest_audit(AUDIT_STREAM)

    def test_retry_until_accepted_with_idempotency_header(self) -> None:
        first = self.relay.run_once()
        self.assertEqual(1, first["retried"])
        self.assertEqual(0, first["sent"])
        for _ in range(2):
            self.clock.advance(2)
            self.relay.run_once()
        self.assertEqual(1, self.outbox.stats()["sent"])
        record = self.outbox.pending()
        self.assertEqual([], record)
        accepted = _ReceiverHandler.received[0]
        self.assertEqual("H-9", accepted["body"]["event"]["details"]["heat_id"])
        self.assertEqual(accepted["body"]["event_id"], accepted["headers"]["Idempotency-Key"])
        self.assertEqual("1", accepted["headers"]["X-Audit-Seq"])
        self.assertFalse(accepted["duplicate"])
        # 对端总共见过 3 次尝试，但只承认了一次
        event_id = accepted["body"]["event_id"]
        self.assertEqual(3, _ReceiverHandler.attempts_by_id[event_id])

    def test_redelivery_after_crash_is_deduplicated_by_receiver(self) -> None:
        # 对端已经处理过，但本地在落 sent 之前崩溃：重投时对端按幂等键返回同一回执。
        _ReceiverHandler.reset(fail_times=0)
        self.relay.run_once()
        self.assertEqual(1, self.outbox.stats()["sent"])
        # 模拟崩溃：把状态文档人为退回 queued，事件信封不变（event_id 不变）。
        state_doc = self.store.require("outbox/state/000000000001")
        payload = dict(state_doc.payload)
        payload["state"] = "queued"
        payload["attempts"] = 0
        self.store.put("outbox/state/000000000001", payload)
        self.relay.run_once()
        accepted = _ReceiverHandler.received[-1]
        self.assertTrue(accepted["duplicate"])
        record = self.outbox.get_record(1)
        self.assertEqual("sent", record.state)
        self.assertTrue(record.receiver_receipt.get("event_id"))

    def test_connection_refused_is_transient(self) -> None:
        transport = HttpTransport("http://127.0.0.1:1/events", timeout_seconds=2, clock=self.clock)
        result = transport.send(self.outbox.entries()[0].envelope)
        self.assertTrue(result.transient)
        self.assertIsNone(result.status_code)


if __name__ == "__main__":
    unittest.main()
