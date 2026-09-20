"""事件外发的 HTTP 接口：中文目标名、URL 解码、补发与对账路由。"""

from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from flashsmelter.console import ConsoleApp, ConsoleServer
from flashsmelter.egress import TargetDispatcher
from flashsmelter.egress.sink import DeliveryResult

from .helpers import make_app, start_furnace


class HttpSink:
    name = "http-memory"
    endpoint = "memory://http-memory"

    def __init__(self) -> None:
        self.received: list[dict] = []
        self.online = True
        self.lock = threading.Lock()

    def deliver(self, envelope, *, timeout: float) -> DeliveryResult:
        with self.lock:
            if not self.online:
                return DeliveryResult(False, False, None, None, "连接被拒绝")
            self.received.append(dict(envelope))
            return DeliveryResult(True, False, 202, envelope["event_id"], "accepted")


class EgressHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = make_app()
        self.sink = HttpSink()
        self.app.egress.add_target(
            TargetDispatcher(
                "上级",
                self.sink,
                self.app.store,
                namespace=self.app.namespace.prefix,
                clock=self.app.clock,
                timeout=2.0,
                max_attempts=0,
            )
        )
        self.console = ConsoleApp(self.app)
        self.server = ConsoleServer(self.console, host="127.0.0.1", port=0)
        self.host, self.port = self.server.start()

    def tearDown(self) -> None:
        self.server.stop()

    def _request(self, method: str, path: str, body: dict | None = None):
        url = f"http://{self.host}:{self.port}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(url, data=data, method=method)
        if data:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8"))

    def test_egress_routes_round_trip_with_chinese_target_name(self) -> None:
        start_furnace(self.app)
        self.app.egress.ingest()

        status, payload = self._request("GET", "/api/egress/status")
        self.assertEqual(200, status)
        self.assertEqual("上级", payload["targets"][0]["target"])
        self.assertGreaterEqual(payload["outbox_length"], 1)

        status, payload = self._request("GET", "/api/egress/events?limit=5")
        self.assertEqual(200, status)
        self.assertGreaterEqual(payload["count"], 1)

        status, payload = self._request("POST", "/api/egress/pump")
        self.assertEqual(200, status)
        self.assertGreaterEqual(payload["targets"][0]["sent"], 1)
        self.assertEqual(len(self.sink.received), payload["outbox_length"])

        # 中文目标名按 URL 编码出现在路径里（真实客户端行为），路由侧解码回「上级」。
        from urllib.parse import quote

        encoded_path = f"/api/egress/targets/{quote('上级')}/attempts?limit=5"
        status, payload = self._request("GET", encoded_path)
        self.assertEqual(200, status)
        self.assertEqual("上级", payload["target"])
        self.assertGreaterEqual(payload["count"], 1)

        status, payload = self._request("GET", "/api/egress/reconcile")
        self.assertEqual(200, status)
        self.assertTrue(payload["ok"], payload["problems"])

    def test_unknown_target_returns_validation_error(self) -> None:
        from urllib.parse import quote

        status, payload = self._request("GET", f"/api/egress/targets/{quote('不存在')}/attempts")
        self.assertEqual(400, status)
        self.assertEqual("validation-error", payload["error"])


if __name__ == "__main__":
    unittest.main()
