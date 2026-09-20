"""逐目标投递状态机。

每个外发目标各自维护一份发送进度：

* 进度是原子文档（``egress/targets/<name>``），记录水位 ``watermark_seq``
  与每事件标记 ``pending/sent/dead``。正常情况下只按发件箱序号单调推进
  水位，因此标记文档不会无限膨胀；
* 另写一条只增不改的尝试流水 ``egress/attempts/<name>``，每次发送尝试
  （成功/失败/放弃）都留痕，配合水位可完整还原「这条事件送了几次、
  每次为什么失败」；
* 退避表在内存里按失败次数增长，``not_before`` 落进 pending 标记，
  断网多久、下次什么时候重试，外部直接可见。
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Mapping, Protocol, runtime_checkable

from ..store import DurableStore
from .outbox import OutboxEvent
from .sink import DeliveryResult

PENDING = "pending"
SENT = "sent"
DEAD = "dead"
STATES = (PENDING, SENT, DEAD)

# 指数退避：1,2,4,8,... 封顶 300 秒。
_BACKOFF_BASE = 1.0
_BACKOFF_CAP = 300.0


def slugify_target(name: str) -> str:
    """目标名可能是中文（如「上级」），落盘键段只允许 ASCII，转成安全短名。"""

    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-")
    if len(slug) > 48:
        slug = slug[:48].strip("-")
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]
    return f"{slug or 'target'}-{digest}"


@runtime_checkable
class SinkPort(Protocol):
    name: str

    def deliver(self, envelope: Mapping[str, Any], *, timeout: float) -> DeliveryResult: ...


class TargetDispatcher:
    """一个外发目标的发送状态机。"""

    def __init__(
        self,
        name: str,
        sink: SinkPort,
        store: DurableStore,
        *,
        namespace: str,
        clock: Any,
        timeout: float,
        max_attempts: int,
    ) -> None:
        self.name = name
        self._sink = sink
        self._store = store
        self._clock = clock
        self._timeout = timeout
        self._max_attempts = max_attempts
        prefix = namespace.replace("/", "_")
        slug = slugify_target(name)
        self._state_key = f"{prefix}/egress/targets/{slug}"
        self._attempt_stream = f"{prefix}/egress/attempts/{slug}"

    # ------------------------------------------------------------------ 状态
    def state(self) -> dict[str, Any]:
        doc = self._load()
        pending = {key: mark for key, mark in doc["marks"].items() if mark["state"] == PENDING}
        dead = {key: mark for key, mark in doc["marks"].items() if mark["state"] == DEAD}
        return {
            "target": self.name,
            "sink": getattr(self._sink, "name", "custom"),
            "endpoint": getattr(self._sink, "endpoint", None),
            "watermark_seq": doc["watermark_seq"],
            "pending": pending,
            "dead": dead,
            "sent_count": doc["sent_count"],
            "attempt_count": self._store.stream_length(self._attempt_stream),
        }

    def attempts(self, *, limit: int = 50) -> list[dict[str, Any]]:
        entries = self._store.read_stream(self._attempt_stream, limit=limit)
        return [dict(entry.payload, seq=entry.seq) for entry in entries]

    # ------------------------------------------------------------------ 投递
    def dispatch_pending(self, events: list[OutboxEvent], *, force: bool = False) -> dict[str, Any]:
        """按发件箱序号顺序尝试所有「已到重试时间」的 pending/dead 事件。

        ``force`` 用于人工「立即重试」，忽略退避与 dead 终态（dead 复活为
        pending 重新计数）。返回本轮投递统计。
        """

        doc = self._load()
        by_seq = {event.seq: event for event in events}
        now = self._clock.timestamp()
        sent = failed = dead = skipped = 0

        for event in events:
            mark = doc["marks"].get(event.event_id)
            if mark is None:
                if event.seq <= doc["watermark_seq"]:
                    continue
                mark = self._new_mark(event)
                doc["marks"][event.event_id] = mark
            state = mark["state"]
            if state == SENT:
                continue
            if not force and state == DEAD:
                skipped += 1
                continue
            if not force and state == PENDING and mark["not_before"] > now:
                skipped += 1
                continue
            if force and state == DEAD:
                mark = dict(mark, state=PENDING, attempts=0, not_before=0.0)
            result = self._sink.deliver(event.envelope, timeout=self._timeout)
            mark["attempts"] += 1
            if result.ok:
                self._mark_sent(doc, mark, event, result)
                sent += 1
                continue
            self._record_attempt(event, result, mark["attempts"])
            if result.permanent or self._exhausted(mark["attempts"]):
                mark.update(state=DEAD, last_error=result.detail, not_before=0.0)
                dead += 1
            else:
                backoff = min(_BACKOFF_BASE * 2 ** (mark["attempts"] - 1), _BACKOFF_CAP)
                mark.update(
                    state=PENDING,
                    last_error=result.detail,
                    last_status=result.status,
                    not_before=now + backoff,
                )
                failed += 1
            doc["marks"][event.event_id] = mark

        self._compact(doc, by_seq)
        self._save(doc)
        return {
            "target": self.name,
            "sent": sent,
            "retry_failed": failed,
            "dead": dead,
            "skipped": skipped,
            "watermark_seq": doc["watermark_seq"],
        }

    def retry_dead(self, events: list[OutboxEvent]) -> dict[str, Any]:
        """人工放行：dead 事件重新进入投递。"""

        return self.dispatch_pending(events, force=True)

    def mark_state(self, event_id: str) -> dict[str, Any] | None:
        return self._load()["marks"].get(event_id)

    # ------------------------------------------------------------------ 内部
    def _mark_sent(
        self,
        doc: dict[str, Any],
        mark: dict[str, Any],
        event: OutboxEvent,
        result: DeliveryResult,
    ) -> None:
        mark.update(
            state=SENT,
            sent_at=self._clock.timestamp_iso(),
            remote_id=result.remote_id,
            last_status=result.status,
            last_error=None,
            not_before=0.0,
        )
        doc["marks"][event.event_id] = mark
        doc["sent_count"] += 1
        self._record_attempt(event, result, mark["attempts"])

    def _record_attempt(self, event: OutboxEvent, result: DeliveryResult, attempts: int) -> None:
        self._store.append(
            self._attempt_stream,
            {
                "event_id": event.event_id,
                "outbox_seq": event.seq,
                "at": self._clock.timestamp_iso(),
                "result": "sent" if result.ok else "failed",
                "permanent": result.permanent,
                "status": result.status,
                "detail": result.detail,
                "attempts": attempts,
            },
        )

    def _exhausted(self, attempts: int) -> bool:
        return self._max_attempts > 0 and attempts >= self._max_attempts

    def _new_mark(self, event: OutboxEvent) -> dict[str, Any]:
        return {
            "state": PENDING,
            "outbox_seq": event.seq,
            "audit_seq": event.audit_seq,
            "attempts": 0,
            "not_before": 0.0,
            "last_error": None,
            "last_status": None,
            "sent_at": None,
            "remote_id": None,
        }

    def _compact(self, doc: dict[str, Any], by_seq: dict[int, OutboxEvent]) -> None:
        """已 sent 且连续在水位之上的标记只保留水位，不逐条占空间。"""

        watermark = doc["watermark_seq"]
        for event in sorted(by_seq.values(), key=lambda item: item.seq):
            mark = doc["marks"].get(event.event_id)
            if mark is None or mark["state"] != SENT or mark["outbox_seq"] != watermark + 1:
                break
            watermark += 1
            doc["marks"].pop(event.event_id, None)
        doc["watermark_seq"] = watermark

    def _load(self) -> dict[str, Any]:
        record = self._store.get(self._state_key)
        if record is None:
            return {"watermark_seq": 0, "sent_count": 0, "marks": {}}
        payload = record.payload
        return {
            "watermark_seq": int(payload.get("watermark_seq", 0)),
            "sent_count": int(payload.get("sent_count", 0)),
            "marks": dict(payload.get("marks") or {}),
        }

    def _save(self, doc: dict[str, Any]) -> None:
        self._store.put(
            self._state_key,
            {
                "watermark_seq": doc["watermark_seq"],
                "sent_count": doc["sent_count"],
                "updated_at": self._clock.timestamp_iso(),
                "marks": doc["marks"],
            },
        )


__all__ = ["TargetDispatcher", "AttemptRecord", "PENDING", "SENT", "DEAD", "STATES"]
