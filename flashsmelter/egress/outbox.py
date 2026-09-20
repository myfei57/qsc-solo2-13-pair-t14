"""发件箱：关键事件的本地持久缓冲。

发件箱是追加型 JSONL 流水（复用 :class:`DurableStore`，带单调序号与逐行
校验和），与审计流一一对应：

* 审计流是本地真相，发件箱只收「策略判定为关键」的事件；
* 每条事件的 ``event_id`` 由审计序号确定性派生，重复收录时靠发件箱末端
  去重保证「一条审计事件只入箱一次」，进而为对账提供稳定的事件集合；
* 收录游标单独落盘（原子文档），重启后从游标之后继续扫描，断点续传。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..store import DurableStore, JournalEntry
from .policy import CriticalEvent, CriticalPolicy, event_id_for

OUTBOX_STREAM = "egress/outbox"
CURSOR_KEY_FALLBACK = "egress/cursor"


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    """发件箱里的一条事件。``seq`` 是发件箱自身的流水序号。"""

    seq: int
    event_id: str
    audit_seq: int
    at: str
    enqueued_at: str
    envelope: Mapping[str, Any]
    payload_checksum: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "audit_seq": self.audit_seq,
            "at": self.at,
            "enqueued_at": self.enqueued_at,
            "payload_checksum": self.payload_checksum,
            "envelope": dict(self.envelope),
        }


class Outbox:
    """扫描审计流、收录关键事件、按序号回读。"""

    def __init__(
        self,
        store: DurableStore,
        policy: CriticalPolicy,
        *,
        namespace: str,
        clock: Any,
    ) -> None:
        self._store = store
        self._policy = policy
        self._namespace = namespace
        self._clock = clock
        self._stream = f"{namespace.replace('/', '_')}/{OUTBOX_STREAM}"
        self._cursor_key = f"{namespace.replace('/', '_')}/{CURSOR_KEY_FALLBACK}"

    @property
    def stream(self) -> str:
        return self._stream

    def admit_new(self, audit: Any, *, batch_size: int = 1000) -> list[OutboxEvent]:
        """把审计流里游标之后的关键事件全部收入发件箱，返回新入箱事件。

        收录与游标推进在同一把存储锁下顺序完成；即使进程在中途崩溃，重启
        后也只会重新扫描——末端去重保证幂等，绝不会产生第二条同号事件。
        ``read_stream`` 按窗口返回最后 N 条，因此极端积压时要循环到游标
        追平，不能只读一轮，否则中间的关键事件会被漏掉。
        """

        admitted: list[OutboxEvent] = []
        while True:
            events = audit.read_events_forward(since_seq=self._cursor(), limit=batch_size)
            if not events:
                break
            for event in events:
                critical = self._policy.from_audit(event.to_dict())
                if critical is None:
                    self._advance_cursor(event.seq)
                    continue
                if self._is_tail_duplicate(critical.event_id):
                    self._advance_cursor(event.seq)
                    continue
                entry = self._append(critical)
                self._advance_cursor(event.seq)
                admitted.append(self._to_event(entry))
            if len(events) < batch_size:
                break
        return admitted

    def read(
        self,
        *,
        since_seq: int = 0,
        limit: int = 1000,
        verify: bool = True,
    ) -> list[OutboxEvent]:
        entries = self._store.read_stream(
            self._stream, since_seq=since_seq, limit=limit, verify=verify
        )
        return [self._to_event(entry) for entry in entries]

    def length(self) -> int:
        return self._store.stream_length(self._stream)

    def cursor(self) -> int:
        return self._cursor()

    # ------------------------------------------------------------------ 内部
    def _append(self, critical: CriticalEvent) -> JournalEntry:
        payload = {
            "event_id": critical.event_id,
            "audit_seq": critical.audit_seq,
            "at": critical.at,
            "envelope": critical.envelope(),
            "payload_checksum": critical.payload_checksum,
        }
        return self._store.append(self._stream, payload)

    def _is_tail_duplicate(self, event_id: str) -> bool:
        """只检查发件箱最后一条：收录严格按审计序号递增，重复必在末端。"""

        tail = self._store.read_stream(self._stream, since_seq=0, limit=1)
        return bool(tail and tail[-1].payload.get("event_id") == event_id)

    def _cursor(self) -> int:
        record = self._store.get(self._cursor_key)
        if record is None:
            return 0
        try:
            return int(record.payload.get("audit_seq", 0))
        except (TypeError, ValueError):  # pragma: no cover - 写入侧保证为整数
            return 0

    def _advance_cursor(self, audit_seq: int) -> None:
        self._store.put(
            self._cursor_key,
            {"audit_seq": int(audit_seq), "updated_at": self._clock.timestamp_iso()},
        )

    def _to_event(self, entry: JournalEntry) -> OutboxEvent:
        payload = entry.payload
        envelope = payload.get("envelope") or {}
        audit_seq = int(payload.get("audit_seq", envelope.get("audit_seq", 0)))
        return OutboxEvent(
            seq=entry.seq,
            event_id=str(payload.get("event_id") or event_id_for(self._namespace, audit_seq)),
            audit_seq=audit_seq,
            at=str(payload.get("at", envelope.get("at", entry.written_at))),
            enqueued_at=str(entry.written_at),
            envelope=envelope,
            payload_checksum=str(payload.get("payload_checksum", "")),
        )


__all__ = ["Outbox", "OutboxEvent", "OUTBOX_STREAM"]
