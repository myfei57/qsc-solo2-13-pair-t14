"""应用集成：关键动作自动入箱、HTTP 接口、verify 对账与跨重启续送。"""

from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from flashsmelter.console import ConsoleApp, ConsoleServer
from flashsmelter.runtime import ManualClock

from .helpers import feed_heat, make_app, make_root, start_furnace


class _AcceptAllHandler(BaseHTTPRequestHandler):
    bodies: list[dict] = []
    lock = threading.Lock()

    def log_message(self, *args):  # noqa: A002
        pass

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        with self.lock:
            type(self).bodies.append(
                {
                    "body": body,
                    "idempotency_key": self.headers.get("Idempotency-Key"),
                    "audit_seq": self.headers.get("X-Audit-Seq"),
                }
            )
        payload = json.dumps({"accepted": True}).encode("utf-8")
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    @classmethod
    def reset(cls) -> None:
        cls.bodies = []


class OutboxIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _AcceptAllHandler)
        cls.host, cls.port = cls.server.server_address[:2]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        _AcceptAllHandler.reset()
        self.root = make_root("flashsmelter-app-outbox-")
        self.clock = ManualClock()
        self.app = make_app(
            root=self.root,
            clock=self.clock,
            outbox_endpoint=f"http://{self.host}:{self.port}/events",
            outbox_retry_backoff_seconds=1.0,
        )
        self.console = ConsoleApp(self.app)
        start_furnace(self.app)

    def tearDown(self) -> None:
        self.app.stop_relay()

    def test_key_action_auto_enqueued_synchronously(self) -> None:
        # 不开 relay：动作完成后事件必须已经在本地发件箱里，不依赖网络。
        feed_heat(self.app, "H-1")
        qualified = [f"{item['envelope']['target'].split('/', 1)[0]}.{item['envelope']['action']}"
                     for item in self.app.outbox_events()]
        self.assertIn("furnace.feed", qualified)
        self.assertIn("conc.inject", qualified)
        stats = self.app.outbox_status()
        self.assertTrue(stats["delivery_enabled"])
        self.assertGreater(stats["queued"], 0)
        self.assertEqual(0, stats["sent"])

    def test_flush_delivers_and_audit_outbox_reconcile(self) -> None:
        feed_heat(self.app, "H-1")
        flushed = self.app.flush_outbox()
        self.assertGreater(flushed["sent"], 0)
        self.assertEqual(flushed["sent"], len(_AcceptAllHandler.bodies))
        first = _AcceptAllHandler.bodies[0]
        self.assertEqual(first["body"]["event_id"], first["idempotency_key"])
        self.assertTrue(first["audit_seq"])
        self.assertEqual("smelter/line1", first["body"]["namespace"])
        report = self.app.outbox_reconcile()
        self.assertTrue(report["ok"], msg=report)
        self.assertEqual([], report["undelivered"])
        self.assertEqual(report["key_audit_events"], report["outbox_events"])

    def test_rejected_non_key_event_is_not_enqueued(self) -> None:
        # 顺序错位的放渣被门控拒绝：slag.tap 只外发成功，拒绝尝试不进箱。
        before = len(self.app.outbox_events())
        with self.assertRaises(Exception):
            self.app.slag.tap("tester", heat_id="H-X", target_tons=5.0)
        self.assertEqual(before, len(self.app.outbox_events()))

    def test_backlog_survives_restart_and_resumes_in_order(self) -> None:
        feed_heat(self.app, "H-1")
        queued_ids = [item["envelope"]["event_id"] for item in self.app.outbox_events()]
        # 模拟断网期间重启：新 Application 指向同一持久目录。
        app2 = make_app(
            root=self.root,
            clock=self.clock,
            outbox_endpoint=f"http://{self.host}:{self.port}/events",
        )
        try:
            pending = app2.outbox_pending()
            self.assertEqual(len(queued_ids), len(pending))
            flushed = app2.flush_outbox()
            self.assertEqual(len(queued_ids), flushed["sent"])
        finally:
            app2.stop_relay()
        delivered_ids = [item["body"]["event_id"] for item in _AcceptAllHandler.bodies]
        self.assertEqual(queued_ids, delivered_ids)

    def test_verify_includes_outbox_reconciliation(self) -> None:
        feed_heat(self.app, "H-1")
        report = self.app.verify()
        # 有未投递事件不影响 ok（那是「还没送」，不是「对不上」）；对账本身无差异。
        self.assertTrue(report["ok"], msg=report)
        self.assertTrue(report["outbox_reconcile"]["ok"])
        self.app.flush_outbox()

    # ------------------------------------------------------------- HTTP 接口
    def test_console_outbox_endpoints(self) -> None:
        server = ConsoleServer(self.console, host="127.0.0.1", port=0)
        host, port = server.start()
        try:
            import urllib.request

            def get(path):
                with urllib.request.urlopen(f"http://{host}:{port}{path}", timeout=5) as response:
                    return response.status, json.loads(response.read().decode("utf-8"))

            def post(path, body=None):
                data = json.dumps(body or {}).encode("utf-8")
                request = urllib.request.Request(
                    f"http://{host}:{port}{path}", data=data, method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(request, timeout=5) as response:
                    return response.status, json.loads(response.read().decode("utf-8"))

            status, payload = get("/api/outbox")
            self.assertEqual(200, status)
            self.assertIn("queued", payload)
            self.assertIn("oldest_pending_age_seconds", payload)
            status, flushed = post("/api/outbox/flush")
            self.assertEqual(200, status)
            self.assertGreater(flushed["flushed"]["sent"], 0)
            status, events = get("/api/outbox/events?limit=5")
            self.assertEqual(200, status)
            self.assertLessEqual(events["count"], 5)
            status, reconcile = get("/api/outbox/reconcile")
            self.assertEqual(200, status)
            self.assertTrue(reconcile["ok"])
            status, dead = get("/api/outbox/dead")
            self.assertEqual(200, status)
            self.assertEqual(0, dead["count"])
        finally:
            server.stop()


class OutboxDisabledTest(unittest.TestCase):
    def test_no_endpoint_means_local_retention_only(self) -> None:
        app = make_app(root=make_root("flashsmelter-outbox-off-"), clock=ManualClock())
        try:
            self.assertFalse(app.relay.delivery_enabled)
            self.assertFalse(app.start_relay())
            start_furnace(app)
            feed_heat(app, "H-1")
            self.assertGreater(app.outbox_status()["queued"], 0)
            counts = app.flush_outbox()
            self.assertEqual(0, counts["attempts"])
        finally:
            app.stop_relay()


if __name__ == "__main__":
    unittest.main()
