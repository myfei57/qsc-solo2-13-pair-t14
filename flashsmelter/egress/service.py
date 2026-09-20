"""外发服务：把策略、发件箱、各目标投递器编排成一个整体。

调用方（应用装配层、CLI、HTTP）只跟这一层打交道：

* :meth:`pump_once` 收录 + 逐目标投递一轮，是后台泵与人工补发的同一入口；
* :meth:`status` / :meth:`events` / :meth:`attempts` 回答「没送到的在哪」；
* :meth:`reconcile` 回答「送出去的和本地记录对不对得上」。
"""

from __future__ import annotations

from typing import Any, Mapping

from ..errors import ValidationError
from ..runtime import Clock
from ..store import DurableStore
from .dispatcher import PENDING, SENT, TargetDispatcher
from .outbox import Outbox, OutboxEvent
from .policy import CriticalPolicy, payload_checksum_of


def body_of_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """从外发报文取出参与校验和的业务字段。"""

    return {
        "at": envelope.get("at", ""),
        "namespace": envelope.get("namespace", ""),
        "component": envelope.get("component", ""),
        "action": envelope.get("action", ""),
        "target": envelope.get("target", ""),
        "outcome": envelope.get("outcome", ""),
        "actor": envelope.get("actor", ""),
        "correlation_id": envelope.get("correlation_id", ""),
        "details": dict(envelope.get("details") or {}),
    }


class EgressService:
    def __init__(
        self,
        store: DurableStore,
        *,
        namespace: str,
        clock: Clock,
        policy: CriticalPolicy | None = None,
        targets: Mapping[str, TargetDispatcher] | None = None,
    ) -> None:
        self._store = store
        self._namespace = namespace
        self._clock = clock
        self.policy = policy or CriticalPolicy(namespace)
        self.outbox = Outbox(store, self.policy, namespace=namespace, clock=clock)
        self._targets: dict[str, TargetDispatcher] = dict(targets or {})
        self._audit: Any = None

    # ------------------------------------------------------------------ 目标
    def add_target(self, dispatcher: TargetDispatcher) -> None:
        if dispatcher.name in self._targets:
            raise ValueError(f"外发目标重复注册: {dispatcher.name}")
        self._targets[dispatcher.name] = dispatcher

    @property
    def target_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._targets))

    def _target(self, name: str) -> TargetDispatcher:
        try:
            return self._targets[name]
        except KeyError:
            raise ValidationError(
                "未知外发目标", details={"target": name, "known": list(self.target_names)}
            ) from None

    # ------------------------------------------------------------------ 主流程
    def bind_audit(self, audit: Any) -> None:
        """应用装配时注入审计日志，避免 egress → audit 的包级循环依赖。"""

        self._audit = audit

    def ingest(self) -> list[OutboxEvent]:
        """把审计流游标之后的新关键事件收入发件箱。"""

        if self._audit is None:
            return []
        return self.outbox.admit_new(self._audit)

    def pump_once(self, *, force: bool = False) -> dict[str, Any]:
        """收录一轮、再让每个目标各投递一轮。"""

        admitted = self.ingest()
        all_events = self.outbox.read(limit=1_000_000)
        results = [
            target.dispatch_pending(all_events, force=force)
            for target in self._targets.values()
        ]
        return {
            "ingested": len(admitted),
            "outbox_length": self.outbox.length(),
            "targets": results,
        }

    # ------------------------------------------------------------------ 可见性
    def status(self) -> dict[str, Any]:
        return {
            "namespace": self._namespace,
            "enabled": bool(self._targets),
            "audit_cursor_seq": self.outbox.cursor(),
            "outbox_length": self.outbox.length(),
            "critical_actions": list(self.policy.patterns),
            "targets": [self._targets[name].state() for name in self.target_names],
        }

    def events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for event in self.outbox.read(limit=limit):
            envelope = dict(event.envelope)
            rows.append(
                {
                    "seq": event.seq,
                    "event_id": event.event_id,
                    "audit_seq": event.audit_seq,
                    "at": event.at,
                    "enqueued_at": event.enqueued_at,
                    "action": envelope.get("action"),
                    "outcome": envelope.get("outcome"),
                    "severity": envelope.get("severity"),
                    "target": envelope.get("target"),
                    "payload_checksum": event.payload_checksum,
                    "deliveries": {
                        name: self._delivery_view(name, event) for name in self.target_names
                    },
                }
            )
        return rows

    def _delivery_view(self, target: str, event: OutboxEvent) -> dict[str, Any]:
        dispatcher = self._target(target)
        mark = dispatcher.mark_state(event.event_id)
        if mark is not None:
            return {
                "state": mark["state"],
                "attempts": mark["attempts"],
                "not_before": mark.get("not_before"),
                "last_error": mark.get("last_error"),
                "last_status": mark.get("last_status"),
                "sent_at": mark.get("sent_at"),
                "remote_id": mark.get("remote_id"),
            }
        if event.seq <= dispatcher.state()["watermark_seq"]:
            return {"state": SENT, "attempts": None, "compacted": True}
        return {"state": PENDING, "attempts": 0, "compacted": False}

    def attempts(self, target: str, *, limit: int = 50) -> list[dict[str, Any]]:
        return self._target(target).attempts(limit=limit)

    def retry_dead(self, target: str) -> dict[str, Any]:
        all_events = self.outbox.read(limit=1_000_000)
        return self._target(target).retry_dead(all_events)

    # ------------------------------------------------------------------ 对账
    def reconcile(self) -> dict[str, Any]:
        """审计关键事件 ↔ 发件箱 ↔ 各目标 sent 标记，三方对一遍。"""

        problems: list[str] = []

        critical: dict[int, Mapping[str, Any]] = {}
        if self._audit is not None:
            for event in self._audit.read_events(limit=1_000_000):
                critical_event = self.policy.from_audit(event.to_dict())
                if critical_event is not None:
                    critical[critical_event.audit_seq] = critical_event.envelope()

        outbox_events = self.outbox.read(limit=1_000_000)
        by_audit_seq: dict[int, list[OutboxEvent]] = {}
        for event in outbox_events:
            by_audit_seq.setdefault(event.audit_seq, []).append(event)

        missing = sorted(seq for seq in critical if seq not in by_audit_seq)
        for seq in missing:
            problems.append(f"审计关键事件 seq={seq} 未入发件箱")

        duplicates = {seq: items for seq, items in by_audit_seq.items() if len(items) > 1}
        for seq, items in sorted(duplicates.items()):
            problems.append(f"发件箱中审计 seq={seq} 重复收录 {len(items)} 次")

        checksum_bad: list[int] = []
        for seq, items in sorted(by_audit_seq.items()):
            for item in items:
                recomputed = payload_checksum_of(body_of_envelope(item.envelope))
                if item.payload_checksum and recomputed != item.payload_checksum:
                    checksum_bad.append(seq)
                    problems.append(f"发件箱 seq={seq} 载荷校验和与报文不一致")
                if seq in critical:
                    expected = payload_checksum_of(body_of_envelope(critical[seq]))
                    if item.payload_checksum and expected != item.payload_checksum:
                        checksum_bad.append(seq)
                        problems.append(f"发件箱 seq={seq} 载荷与审计记录不一致")

        targets_report: dict[str, Any] = {}
        for name, dispatcher in self._targets.items():
            state = dispatcher.state()
            watermark = state["watermark_seq"]
            sent_seqs = {event.audit_seq for event in outbox_events if event.seq <= watermark}
            pending_seqs: set[int] = set()
            dead_seqs: set[int] = set()
            for mark in state["pending"].values():
                if mark.get("audit_seq") is not None:
                    pending_seqs.add(int(mark["audit_seq"]))
            for mark in state["dead"].values():
                if mark.get("audit_seq") is not None:
                    dead_seqs.add(int(mark["audit_seq"]))
            for event in outbox_events:
                mark = dispatcher.mark_state(event.event_id)
                if mark is not None and mark["state"] == SENT:
                    sent_seqs.add(event.audit_seq)

            known_ids = {event.event_id for event in outbox_events}
            orphan_marks = sorted(
                event_id
                for event_id in (*state["pending"], *state["dead"])
                if event_id not in known_ids
            )
            for event_id in orphan_marks:
                problems.append(f"目标 {name} 存在发件箱查无此事件的标记: {event_id}")

            unsent = sorted(set(critical) - sent_seqs - pending_seqs - dead_seqs)
            for seq in unsent:
                problems.append(f"目标 {name} 缺少审计 seq={seq} 的投递记录")
            if pending_seqs:
                problems.append(
                    f"目标 {name} 有 {len(pending_seqs)} 条事件尚未送达（pending）"
                )
            if dead_seqs:
                problems.append(f"目标 {name} 有 {len(dead_seqs)} 条死信（dead）")

            targets_report[name] = {
                "watermark_seq": watermark,
                "sent": len(sent_seqs),
                "pending": sorted(pending_seqs),
                "dead": sorted(dead_seqs),
                "unsent": unsent,
                "orphan_marks": orphan_marks,
                "attempt_count": state["attempt_count"],
            }

        return {
            "ok": not problems,
            "critical_total": len(critical),
            "outbox_total": len(outbox_events),
            "missing_in_outbox": missing,
            "duplicated": sorted(duplicates),
            "checksum_mismatch": sorted(set(checksum_bad)),
            "targets": targets_report,
            "problems": problems,
        }


__all__ = ["EgressService", "body_of_envelope"]
