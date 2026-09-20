"""关键事件外发：收录、断网补发、幂等、死信可见、三方对账。"""

from __future__ import annotations

import threading
import unittest

from flashsmelter.application import Application
from flashsmelter.config import Settings
from flashsmelter.egress import (
    CriticalPolicy,
    EgressPump,
    EgressService,
    TargetDispatcher,
)
from flashsmelter.egress.dispatcher import DEAD, PENDING, SENT
from flashsmelter.egress.sink import DeliveryResult
from flashsmelter.runtime import ManualClock

from .helpers import make_root, run_heat, start_furnace


class RecordingSink:
    """内存假通道：可切换通/断，记录每次投递与对端已收到的幂等键。"""

    name = "recording"
    endpoint = "memory://recorder"

    def __init__(self, *, fail_permanent: bool = False) -> None:
        self.received: list[dict] = []
        self.seen_event_ids: set[str] = set()
        self.online = True
        self.fail_permanent = fail_permanent
        self.lock = threading.Lock()

    def go_offline(self) -> None:
        self.online = False

    def go_online(self) -> None:
        self.online = True

    def deliver(self, envelope, *, timeout: float) -> DeliveryResult:
        with self.lock:
            event_id = str(envelope["event_id"])
            if event_id in self.seen_event_ids:
                # 模拟对端幂等表：同一事件绝不重复入账。
                return DeliveryResult(True, False, 200, event_id, "duplicate-ignored")
            if self.fail_permanent:
                return DeliveryResult(False, True, 400, None, "HTTP 400")
            if not self.online:
                return DeliveryResult(False, False, None, None, "连接被拒绝")
            self.seen_event_ids.add(event_id)
            self.received.append(dict(envelope))
            return DeliveryResult(True, False, 200, event_id, "accepted")


def build_service(app: Application, sink: RecordingSink, *, max_attempts: int = 0) -> EgressService:
    service = app.egress
    dispatcher = TargetDispatcher(
        "recorder",
        sink,
        app.store,
        namespace=app.namespace.prefix,
        clock=app.clock,
        timeout=2.0,
        max_attempts=max_attempts,
    )
    service.add_target(dispatcher)
    return service


def start_and_ingest(app: Application) -> None:
    """直调组件绕过了 invoke 的收录钩子，测试里显式补一次。"""

    start_furnace(app)
    app.egress.ingest()


class OutboxIngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Application(Settings(root=make_root()), clock=ManualClock())
        self.sink = RecordingSink()
        self.service = build_service(self.app, self.sink)

    def test_critical_actions_are_enqueued_and_noncritical_are_not(self) -> None:
        start_and_ingest(self.app)
        self.app.furnace.status()
        events = self.service.events()
        actions = [event["action"] for event in events]
        self.assertIn("furnace.start", actions)
        # settle 更新不是默认关键动作，不进箱。
        self.app.settler.update("tester", bath_level_m=0.6, slag_thickness_m=0.1, matte_level_m=0.4)
        self.app.egress.ingest()
        events = self.service.events()
        self.assertNotIn("settler.update", [event["action"] for event in events])

    def test_event_id_is_deterministic_across_restarts(self) -> None:
        start_and_ingest(self.app)
        before = {event["event_id"] for event in self.service.events()}
        # 模拟重启：服务对象重建，但落盘流水与游标都在。
        rebuilt = EgressService(
            self.app.store,
            namespace=self.app.namespace.prefix,
            clock=self.app.clock,
            policy=CriticalPolicy(self.app.namespace.prefix),
        )
        rebuilt.bind_audit(self.app.audit)
        rebuilt.ingest()
        after = {event["event_id"] for event in rebuilt.events()}
        self.assertEqual(before, after)

    def test_rejected_actions_are_also_forwarded(self) -> None:
        # 未开炉直接喷吹，必然被门控拒绝；拒绝也是调度要看的关键时刻。
        try:
            self.app.furnace.feed("tester", heat_id="H-X", rate_tph=100.0, tons=10.0)
        except Exception:
            pass
        self.app.egress.ingest()
        events = self.service.events()
        self.assertTrue(any(e["action"] == "furnace.feed" and e["outcome"] == "rejected" for e in events))

    def test_ingest_pages_through_large_backlog_without_gaps(self) -> None:
        # 直接往审计流灌 250 条事件（关键/非关键交替），批大小 20，验证多页不丢。
        for seq in range(1, 251):
            self.app.audit.record(
                actor="batch",
                action="trip" if seq % 2 else "record_reading",
                target="burner",
                outcome="failed" if seq % 2 else "ok",
                correlation_id=f"c{seq}",
            )
        admitted = self.app.egress.outbox.admit_new(self.app.audit, batch_size=20)
        self.assertEqual(125, len(admitted))
        # 事件序号连续、无重复。
        audit_seqs = [event.audit_seq for event in admitted]
        self.assertEqual(audit_seqs, sorted(audit_seqs))
        self.assertEqual(len(audit_seqs), len(set(audit_seqs)))
        self.assertEqual(self.app.audit.length(), self.app.egress.outbox.cursor())
        # 再收录一轮：没有新事件，幂等不重入箱。
        again = self.app.egress.outbox.admit_new(self.app.audit, batch_size=20)
        self.assertEqual([], again)


class OfflineBufferAndResumeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Application(Settings(root=make_root()), clock=ManualClock())
        self.sink = RecordingSink()
        self.service = build_service(self.app, self.sink)

    def test_events_buffer_while_offline_and_flush_after_recovery(self) -> None:
        self.sink.go_offline()
        start_and_ingest(self.app)
        first = self.service.pump_once()
        self.assertEqual(0, len(self.sink.received))
        pending = self.service.events()
        self.assertTrue(all(row["deliveries"]["recorder"]["state"] == PENDING for row in pending))

        # 断网期间继续生产关键事件，全部在发件箱里垫着。
        run_heat(self.app, heat_id="H-1")
        self.app.egress.ingest()
        self.service.pump_once()
        self.assertEqual(0, len(self.sink.received))

        # 恢复后一轮补齐，顺序与发件箱一致。
        self.sink.go_online()
        self.app.clock.advance(60)
        result = self.service.pump_once()
        self.assertEqual(0, result["targets"][0]["retry_failed"])
        delivered_ids = [event["event_id"] for event in self.sink.received]
        outbox_ids = [event["event_id"] for event in self.service.events()]
        self.assertEqual(outbox_ids, delivered_ids)

    def test_succeded_events_are_never_sent_again(self) -> None:
        start_and_ingest(self.app)
        self.service.pump_once()
        sent_once = len(self.sink.received)
        # 再泵若干轮，包括强制补发，都不应重复发送。
        self.service.pump_once()
        self.service.pump_once(force=True)
        self.assertEqual(sent_once, len(self.sink.received))
        for row in self.service.events():
            self.assertEqual(SENT, row["deliveries"]["recorder"]["state"])

    def test_ack_loss_is_neutralized_by_remote_idempotency(self) -> None:
        # 对端已收但应答丢失：本地仍以为 pending，重发时对端幂等表挡下。
        start_and_ingest(self.app)

        class FlakySink(RecordingSink):
            def deliver(self, envelope, *, timeout: float) -> DeliveryResult:
                event_id = str(envelope["event_id"])
                if event_id not in self.seen_event_ids:
                    self.seen_event_ids.add(event_id)
                    self.received.append(dict(envelope))
                    return DeliveryResult(False, False, None, None, "应答在网络中丢失")
                return super().deliver(envelope, timeout=timeout)

        flaky = FlakySink()
        dispatcher = TargetDispatcher(
            "flaky", flaky, self.app.store,
            namespace=self.app.namespace.prefix, clock=self.app.clock,
            timeout=1.0, max_attempts=0,
        )
        self.service.add_target(dispatcher)
        self.service.pump_once()
        self.app.clock.advance(60)
        self.service.pump_once()
        self.service.pump_once()  # 再多补发一轮也无所谓
        # 对端业务上每条事件只入账一次（无重复）。
        self.assertEqual(len(self.service.events()), len(flaky.received))
        self.assertEqual(
            len(flaky.received), len({item["event_id"] for item in flaky.received})
        )

    def test_backoff_delays_retry_and_is_visible(self) -> None:
        self.sink.go_offline()
        start_and_ingest(self.app)
        self.service.pump_once()
        row = self.service.events()[0]
        mark = row["deliveries"]["recorder"]
        self.assertEqual(1, mark["attempts"])
        self.assertIsNotNone(mark["not_before"])
        self.assertGreater(mark["not_before"], self.app.clock.timestamp())
        # 未到退避时间，下一轮直接跳过。
        skipped = self.service.pump_once()["targets"][0]["skipped"]
        self.assertGreaterEqual(skipped, 1)
        self.app.clock.advance(60)
        self.sink.go_online()
        self.service.pump_once()
        self.assertEqual(len(self.service.events()), len(self.sink.received))


class DeadLetterTest(unittest.TestCase):
    def test_permanent_failure_becomes_dead_and_can_be_revived(self) -> None:
        app = Application(Settings(root=make_root()), clock=ManualClock())
        sink = RecordingSink(fail_permanent=True)
        service = build_service(app, sink)
        start_furnace(app)
        app.egress.ingest()
        service.pump_once()
        status = service.status()["targets"][0]
        self.assertEqual(len(service.events()), len(status["dead"]))
        dead_id = next(iter(status["dead"]))

        # 死信在事件列表与 attempts 里都看得见。
        row = next(event for event in service.events() if event["event_id"] == dead_id)
        self.assertEqual(DEAD, row["deliveries"]["recorder"]["state"])
        attempts = service.attempts("recorder")
        self.assertTrue(any(item["event_id"] == dead_id for item in attempts))

        # 修好通道后人工放行，dead 复活并送达。
        sink.fail_permanent = False
        sink.go_online()
        result = service.retry_dead("recorder")  # force=True 忽略退避
        self.assertEqual(len(service.events()), result["sent"])
        self.assertEqual(0, len(service.status()["targets"][0]["dead"]))

    def test_exhausted_retries_become_dead(self) -> None:
        app = Application(Settings(root=make_root()), clock=ManualClock())
        sink = RecordingSink()
        sink.go_offline()
        service = build_service(app, sink, max_attempts=2)
        start_furnace(app)
        app.egress.ingest()
        service.pump_once()
        app.clock.advance(60)
        service.pump_once()
        self.assertEqual(
            len(service.events()), len(service.status()["targets"][0]["dead"])
        )


class ReconcileTest(unittest.TestCase):
    def test_healthy_pipeline_reconciles_clean(self) -> None:
        app = Application(Settings(root=make_root()), clock=ManualClock())
        sink = RecordingSink()
        service = build_service(app, sink)
        start_furnace(app)
        run_heat(app, heat_id="H-1")
        app.egress.ingest()
        service.pump_once()
        report = service.reconcile()
        self.assertTrue(report["ok"], report["problems"])
        self.assertEqual(report["critical_total"], report["outbox_total"])
        target_report = report["targets"]["recorder"]
        self.assertEqual(0, len(target_report["unsent"]))
        self.assertEqual(report["critical_total"], target_report["sent"])

    def test_reconcile_flags_undelivered_events(self) -> None:
        app = Application(Settings(root=make_root()), clock=ManualClock())
        sink = RecordingSink()
        service = build_service(app, sink)
        start_furnace(app)
        sink.go_offline()
        run_heat(app, heat_id="H-1")
        app.egress.ingest()
        service.pump_once()
        report = service.reconcile()
        # 事件在 pending 里不算丢失，但目标整体尚未送达，报告会如实呈现。
        self.assertFalse(report["ok"])
        self.assertTrue(any("recorder" in problem for problem in report["problems"]))

    def test_checksum_tampering_is_detected(self) -> None:
        import json

        app = Application(Settings(root=make_root()), clock=ManualClock())
        sink = RecordingSink()
        service = build_service(app, sink)
        start_furnace(app)
        app.egress.ingest()
        # 直接篡改发件箱流水行，模拟落盘内容被改动。
        stream = service.outbox.stream
        path = app.store.journal_root / (stream.replace("/", "/") + ".jsonl")
        lines = path.read_text(encoding="utf-8").splitlines()
        entry = json.loads(lines[0])
        entry["payload"]["envelope"]["details"]["tampered"] = True
        lines[0] = json.dumps(entry, ensure_ascii=False)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        # 校验和失配会在读取时先炸（平台级完整性保护）；对账读关闭校验时
        # 也必须能发现内容与审计不一致。
        with self.assertRaises(Exception):
            service.reconcile()


class PumpTest(unittest.TestCase):
    def test_background_pump_flushes_after_recovery(self) -> None:
        app = Application(Settings(root=make_root(), egress_pump_interval_seconds=0.05), clock=ManualClock())
        sink = RecordingSink()
        service = build_service(app, sink)
        sink.go_offline()
        pump = EgressPump(service, interval_seconds=0.05)
        pump.start()
        try:
            start_furnace(app)
            app.egress.ingest()
            pump.stop()
            self.assertEqual(0, len(sink.received))
            sink.go_online()
            app.clock.advance(60)
            pump2 = EgressPump(service, interval_seconds=0.05)
            pump2.start()
            pump2.stop(timeout=2.0)
            self.assertGreaterEqual(len(sink.received), 1)
        finally:
            pump.stop()
            pump2.stop()


if __name__ == "__main__":
    unittest.main()
