"""CLI outbox 子命令：status / events / reconcile 在真实子进程里可用。"""

from __future__ import annotations

import json
import unittest

from .helpers import make_root
from .test_cli import run_cli

START_PARAMS = {
    "drum_level": 0.6,
    "fuel_pressure_kpa": 200.0,
    "air_flow_nm3h": 5200.0,
    "oxygen_baseline": 0.62,
    "oxygen_baseline_source": "analyzer-a",
    "oxygen_target": 0.62,
    "oxygen_flow_nm3h": 9000.0,
}


class CliOutboxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = make_root("flashsmelter-cli-outbox-")

    def _json(self, completed):
        self.assertEqual(0, completed.returncode, msg=completed.stderr or completed.stdout)
        return json.loads(completed.stdout)

    def test_outbox_status_events_and_reconcile(self) -> None:
        status = run_cli("outbox", "status", root=self.root)
        payload = self._json(status)
        self.assertFalse(payload["delivery_enabled"])
        self.assertIsNone(payload["endpoint"])
        self.assertEqual(0, payload["total"])

        started = run_cli(
            "call", "furnace.start", "--params-json", json.dumps(START_PARAMS), root=self.root
        )
        self.assertEqual(0, started.returncode, msg=started.stderr)

        events = run_cli("outbox", "events", root=self.root)
        payload = self._json(events)
        qualified = {
            f"{item['envelope']['component']}.{item['envelope']['action']}" for item in payload["events"]
        }
        self.assertIn("furnace.start", qualified)
        self.assertIn("burner.ignite", qualified)

        pending = run_cli("outbox", "pending", root=self.root)
        self.assertTrue(self._json(pending)["pending"])

        flush = run_cli("outbox", "flush", root=self.root)
        # 未配置端点：只补扫，不触网
        self.assertEqual(0, self._json(flush)["flushed"]["attempts"])

        reconcile = run_cli("outbox", "reconcile", root=self.root)
        report = self._json(reconcile)
        self.assertTrue(report["ok"])
        self.assertGreater(report["key_audit_events"], 0)
        # 未投递事件在 reconcile 里可见
        self.assertGreater(len(report["undelivered"]), 0)

        verify = run_cli("verify", root=self.root)
        self.assertEqual(0, verify.returncode, msg=verify.stdout)
        self.assertTrue(self._json(verify)["outbox_reconcile"]["ok"])

    def test_outbox_dead_and_retry_on_empty(self) -> None:
        dead = run_cli("outbox", "dead", root=self.root)
        self.assertEqual(0, dead.returncode)
        self.assertEqual([], self._json(dead)["dead"])
        # 不存在的序号复位失败
        retry = run_cli("outbox", "retry", "999", root=self.root)
        self.assertEqual(1, retry.returncode)
        self.assertEqual("not-found", json.loads(retry.stdout)["error"])

    def test_outbox_bare_command_prints_status(self) -> None:
        completed = run_cli("outbox", root=self.root)
        self.assertEqual(0, completed.returncode)
        self.assertIn("queued", completed.stdout)


if __name__ == "__main__":
    unittest.main()
