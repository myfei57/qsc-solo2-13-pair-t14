"""事务发件箱（durable outbox）。

关键事件与工艺动作共用同一个 :class:`~flashsmelter.store.DurableStore`：动作的审计
流水行先 fsync 落盘，事件信封随即进同库的发件箱流水——业务记录与外发意图在本地
是同一套持久化事实，不存在「动作做了但外发意图丢了」的窗口。

投递状态单独存成状态文档（``outbox/state/<audit_seq>``），与不可变的事件流水分离：
重试、ack、死信只会推进状态文档的版本，事件本身永不被原地改写。状态机::

    queued -> sending -> sent
                       \\-> queued （暂时性失败：网络、429、5xx，退避后重试）
                       \\-> dead    （永久性失败或超过最大尝试次数，需人工 retry）

游标文档 ``outbox/cursor`` 记录「已扫到审计流水第几行」，后台只做增量扫描；对账
接口仍可全量比对审计流水与发件箱流水，给出漏发/孤儿两类差异。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..errors import NotFoundError, PersistenceError, ValidationError
from ..runtime import Clock
from ..store import DurableStore, JournalEntry
from ..store.codec import checksum_of
from .envelope import attach_audit_checksum, build_envelope, fingerprint_of
from .selector import KeyEventSelector

OUTBOX_STREAM = "outbox/events"
QUEUED = "queued"
SENDING = "sending"
SENT = "sent"
DEAD = "dead"
DELIVERY_STATES = (QUEUED, SENDING, SENT, DEAD)


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """一次投递尝试的结论。传输层回答「成功 / 暂时性失败 / 永久性失败」。"""

    outcome: str  # ok | transient | permanent
    status_code: int | None = None
    reason: str = ""
    acknowledged_at: str = ""
    receiver_receipt: Mapping[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.outcome == "ok"

    @property
    def transient(self) -> bool:
        return self.outcome == "transient"


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """发件箱流水里的一条事件（不可变）。"""

    seq: int
    envelope: Mapping[str, Any]
    written_at: str
    checksum: str

    @property
    def event_id(self) -> str:
        return str(self.envelope["event_id"])

    @property
    def audit_seq(self) -> int:
        return int(self.envelope["audit_seq"])

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "written_at": self.written_at,
            "checksum": self.checksum,
            "envelope": dict(self.envelope),
        }


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    """某条事件当前的投递状态（可变，按版本推进）。"""

    audit_seq: int
    event_id: str
    state: str
    attempts: int
    next_attempt_after: float
    last_error: str
    last_status_code: int | None
    sent_at: str
    acknowledged_at: str
    receiver_receipt: Mapping[str, Any] | None
    enqueued_epoch: float
    version: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "audit_seq": self.audit_seq,
            "event_id": self.event_id,
            "state": self.state,
            "attempts": self.attempts,
            "next_attempt_after": self.next_attempt_after,
            "last_error": self.last_error,
            "last_status_code": self.last_status_code,
            "sent_at": self.sent_at,
            "acknowledged_at": self.acknowledged_at,
            "receiver_receipt": dict(self.receiver_receipt or {}),
            "enqueued_epoch": self.enqueued_epoch,
            "version": self.version,
        }


def _state_key(audit_seq: int) -> str:
    # 定长补零保证 list_keys 排序即投递顺序。
    return f"outbox/state/{audit_seq:012d}"


def _cursor_key() -> str:
    return "outbox/cursor"


class Outbox:
    """关键事件的持久发件箱：入箱、状态推进、扫描与对账。"""

    def __init__(
        self,
        store: DurableStore,
        selector: KeyEventSelector,
        namespace,
        clock: Clock,
        *,
        event_decoder=None,
        max_attempts: int = 20,
    ) -> None:
        self._store = store
        self._selector = selector
        self._namespace = namespace
        self._clock = clock
        self._event_decoder = event_decoder
        self._max_attempts = max_attempts

    # ------------------------------------------------------------- 入箱
    def ingest_audit(self, audit_stream: str, *, batch_limit: int = 500) -> int:
        """增量扫描审计流水，把新出现的关键事件入箱。返回新入箱条数。

        这是兜底路径：正常在动作完成后由 :meth:`enqueue` 立即入箱；即便那条通知
        因任何原因没走到，本扫描也会在一个轮询周期内补齐。
        """

        cursor = self._read_cursor()
        entries = self._store.read_stream(audit_stream, limit=batch_limit, since_seq=cursor)
        ingested = 0
        for entry in entries:
            event = self._audit_event(entry)
            if self._selector.is_key_event(event):
                self._enqueue_entry(entry)
                ingested += 1
            cursor = entry.seq
        if cursor != self._read_cursor():
            self._write_cursor(cursor)
        return ingested

    def enqueue_audit_entry(self, entry: JournalEntry) -> OutboxEntry | None:
        """动作落盘后立即调用：命中关键事件目录才入箱，幂等。"""

        event = self._audit_event(entry)
        if not self._selector.is_key_event(event):
            return None
        return self._enqueue_entry(entry)

    def _enqueue_entry(self, audit_entry: JournalEntry) -> OutboxEntry:
        event = self._audit_event(audit_entry)
        existing = self._store.get(_state_key(event.seq))
        if existing is not None:
            outbox_seq = int(existing.payload.get("outbox_seq", 0))
            record = self._read_entry(outbox_seq)
            return record
        envelope = build_envelope(event, namespace=self._namespace, clock=self._clock)
        envelope = attach_audit_checksum(envelope, audit_entry)
        journal_entry = self._store.append(OUTBOX_STREAM, envelope)
        state_payload = {
            "audit_seq": event.seq,
            "outbox_seq": journal_entry.seq,
            "event_id": envelope["event_id"],
            "state": QUEUED,
            "attempts": 0,
            "next_attempt_after": 0.0,
            "last_error": "",
            "last_status_code": None,
            "sent_at": "",
            "acknowledged_at": "",
            "receiver_receipt": None,
            "created_at": self._clock.timestamp_iso(),
            "enqueued_epoch": self._clock.timestamp(),
        }
        self._store.put(_state_key(event.seq), state_payload)
        return OutboxEntry(
            seq=journal_entry.seq,
            envelope=journal_entry.payload,
            written_at=journal_entry.written_at,
            checksum=journal_entry.checksum,
        )

    # ------------------------------------------------------------- 投递取活
    def due(self, *, now: float | None = None, limit: int = 50) -> list[tuple[OutboxEntry, DeliveryRecord]]:
        """取该投递的事件：queued，或退避已到期的重试项。按事件顺序返回。

        严格按顺序取活：前一条没送出去（暂时性失败）时，后面的不跳过——对调度侧
        来说「喷吹」早于「放铜」的次序不能被网络抖动打乱。
        """

        moment = self._clock.timestamp() if now is None else now
        due_items: list[tuple[OutboxEntry, DeliveryRecord]] = []
        for record in self._iter_delivery():
            if record.state in (QUEUED, SENDING):
                # 退避未到期则跳过；严格按 audit_seq 顺序取活，不越过未到期项
                # （对调度侧来说「喷吹」早于「放铜」的次序不能被网络抖动打乱）。
                if record.state == QUEUED and record.next_attempt_after > moment:
                    break
                if len(due_items) < limit:
                    due_items.append((self._read_entry_by_audit(record.audit_seq), record))
            if len(due_items) >= limit:
                break
        return due_items

    def claim(self, record: DeliveryRecord) -> DeliveryRecord:
        """把一条事件置为 sending：取活到确认之间崩溃也能在下次扫描被捞回。"""

        return self._update_state(record, state=SENDING)

    def mark_sent(self, record: DeliveryRecord, result: DeliveryResult) -> DeliveryRecord:
        return self._update_state(
            record,
            state=SENT,
            attempts=record.attempts + 1,
            last_status_code=result.status_code,
            last_error="",
            sent_at=self._clock.timestamp_iso(),
            acknowledged_at=result.acknowledged_at or self._clock.timestamp_iso(),
            receiver_receipt=dict(result.receiver_receipt or {}),
        )

    def mark_retry(self, record: DeliveryRecord, result: DeliveryResult, *, backoff_seconds: float) -> DeliveryRecord:
        attempts = record.attempts + 1
        if attempts >= self._max_attempts:
            return self._update_state(
                record,
                state=DEAD,
                attempts=attempts,
                next_attempt_after=0.0,
                last_status_code=result.status_code,
                last_error=result.reason or "transient-failure",
            )
        return self._update_state(
            record,
            state=QUEUED,
            attempts=attempts,
            next_attempt_after=self._clock.timestamp() + backoff_seconds,
            last_status_code=result.status_code,
            last_error=result.reason or "transient-failure",
        )

    def mark_dead(self, record: DeliveryRecord, result: DeliveryResult) -> DeliveryRecord:
        return self._update_state(
            record,
            state=DEAD,
            attempts=record.attempts + 1,
            next_attempt_after=0.0,
            last_status_code=result.status_code,
            last_error=result.reason or "permanent-failure",
        )

    def revive(self, audit_seq: int) -> DeliveryRecord:
        """死信人工复位：清零退避重新进入队列。"""

        record = self._read_record(audit_seq)
        if record.state != DEAD:
            raise ValidationError("只有死信状态的事件可以复位", details={"audit_seq": audit_seq, "state": record.state})
        return self._update_state(
            record,
            state=QUEUED,
            next_attempt_after=0.0,
            last_error=record.last_error,
        )

    # ------------------------------------------------------------- 查询
    def pending(self) -> list[DeliveryRecord]:
        return [record for record in self._iter_delivery() if record.state in (QUEUED, SENDING)]

    def dead(self) -> list[DeliveryRecord]:
        return [record for record in self._iter_delivery() if record.state == DEAD]

    def get_record(self, audit_seq: int) -> DeliveryRecord:
        return self._read_record(audit_seq)

    def entries(self, *, limit: int = 100, verify: bool = True) -> list[OutboxEntry]:
        journal_entries = self._store.read_stream(OUTBOX_STREAM, limit=limit, verify=verify)
        return [
            OutboxEntry(seq=item.seq, envelope=item.payload, written_at=item.written_at, checksum=item.checksum)
            for item in journal_entries
        ]

    def stats(self) -> dict[str, Any]:
        counts = {QUEUED: 0, SENDING: 0, SENT: 0, DEAD: 0}
        oldest_pending_age: float | None = None
        now = self._clock.timestamp()
        for record in self._iter_delivery():
            counts[record.state] = counts.get(record.state, 0) + 1
            if record.state in (QUEUED, SENDING):
                age = now - record.enqueued_epoch
                if oldest_pending_age is None or age > oldest_pending_age:
                    oldest_pending_age = age
        return {
            "total": sum(counts.values()),
            "queued": counts[QUEUED],
            "sending": counts[SENDING],
            "sent": counts[SENT],
            "dead": counts[DEAD],
            "oldest_pending_age_seconds": None if oldest_pending_age is None else round(max(oldest_pending_age, 0.0), 3),
            "cursor": self._read_cursor(),
            "max_attempts": self._max_attempts,
            "selector_rules": list(self._selector.patterns),
        }

    # ------------------------------------------------------------- 对账
    def reconcile(self, audit_stream: str, *, limit: int = 1_000_000) -> dict[str, Any]:
        """逐条比对本地审计关键事件与发件箱记录。

        * ``missing``：审计里是关键事件，但发件箱没有对应记录（漏发）；
        * ``orphan``：发件箱有记录，但审计行对不上（串号/被删）；
        * ``tampered``：信封内审计校验和与审计行实际校验和不一致。
        """

        audit_entries = {entry.seq: entry for entry in self._store.read_stream(audit_stream, limit=limit)}
        # 对账时关闭流水行校验和：被篡改的行正是要报出来的差异，不能让读取先抛错。
        outbox_entries = self.entries(limit=limit, verify=False)
        outbox_by_audit: dict[int, OutboxEntry] = {}
        for entry in outbox_entries:
            outbox_by_audit[entry.audit_seq] = entry

        missing: list[int] = []
        tampered: list[int] = []
        for seq, audit_entry in audit_entries.items():
            event = self._audit_event(audit_entry)
            if not self._selector.is_key_event(event):
                continue
            outbox_entry = outbox_by_audit.get(seq)
            if outbox_entry is None:
                missing.append(seq)
                continue
            if outbox_entry.envelope.get("audit_checksum") != audit_entry.checksum:
                tampered.append(seq)
                continue
            body = outbox_entry.envelope.get("event", {})
            if fingerprint_of(body) != outbox_entry.envelope.get("payload_checksum"):
                tampered.append(seq)
        orphan = sorted(seq for seq in outbox_by_audit if seq not in audit_entries)
        undelivered = sorted(
            record.audit_seq
            for record in self._iter_delivery()
            if record.state in (QUEUED, SENDING, DEAD)
        )
        return {
            "ok": not missing and not orphan and not tampered,
            "key_audit_events": sum(
                1 for entry in audit_entries.values() if self._selector.is_key_event(self._audit_event(entry))
            ),
            "outbox_events": len(outbox_entries),
            "missing": missing,
            "orphan": orphan,
            "tampered": tampered,
            "undelivered": undelivered,
        }

    # ------------------------------------------------------------- 内部
    def _audit_event(self, entry: JournalEntry):
        if self._event_decoder is None:
            raise PersistenceError("发件箱缺少审计事件解码器", details={"audit_seq": entry.seq})
        return self._event_decoder(entry)

    def _read_cursor(self) -> int:
        record = self._store.get(_cursor_key())
        return 0 if record is None else int(record.payload.get("audit_seq", 0))

    def _write_cursor(self, seq: int) -> None:
        self._store.put(_cursor_key(), {"audit_seq": seq, "updated_at": self._clock.timestamp_iso()})

    def _read_entry(self, outbox_seq: int) -> OutboxEntry:
        entry = self._store.read_stream_entry(OUTBOX_STREAM, outbox_seq)
        if entry is None:
            raise PersistenceError("发件箱流水缺少对应事件", details={"outbox_seq": outbox_seq})
        return OutboxEntry(
            seq=entry.seq, envelope=entry.payload, written_at=entry.written_at, checksum=entry.checksum
        )

    def _read_entry_by_audit(self, audit_seq: int) -> OutboxEntry:
        record = self._store.require(_state_key(audit_seq))
        return self._read_entry(int(record.payload["outbox_seq"]))

    def _read_record(self, audit_seq: int) -> DeliveryRecord:
        record = self._store.get(_state_key(audit_seq))
        if record is None:
            raise NotFoundError("发件箱中没有该事件", details={"audit_seq": audit_seq})
        return self._to_record(record.payload, record.version)

    def _iter_delivery(self) -> list[DeliveryRecord]:
        records: list[DeliveryRecord] = []
        for key in self._store.list_keys("outbox/state/"):
            doc = self._store.get(key)
            if doc is not None:
                records.append(self._to_record(doc.payload, doc.version))
        records.sort(key=lambda item: item.audit_seq)
        return records

    def _to_record(self, payload: Mapping[str, Any], version: int) -> DeliveryRecord:
        state = str(payload.get("state", QUEUED))
        if state not in DELIVERY_STATES:
            raise PersistenceError("发件箱状态不被识别", details={"state": state})
        return DeliveryRecord(
            audit_seq=int(payload["audit_seq"]),
            event_id=str(payload["event_id"]),
            state=state,
            attempts=int(payload.get("attempts", 0)),
            next_attempt_after=float(payload.get("next_attempt_after", 0.0) or 0.0),
            last_error=str(payload.get("last_error", "")),
            last_status_code=payload.get("last_status_code"),
            sent_at=str(payload.get("sent_at", "")),
            acknowledged_at=str(payload.get("acknowledged_at", "")),
            receiver_receipt=payload.get("receiver_receipt"),
            enqueued_epoch=float(payload.get("enqueued_epoch", 0.0) or 0.0),
            version=version,
        )

    def _update_state(self, record: DeliveryRecord, *, state: str, **changes: Any) -> DeliveryRecord:
        if state not in DELIVERY_STATES:  # pragma: no cover - 内部调用恒合法
            raise ValidationError("非法的投递状态", details={"state": state})
        doc = self._store.require(_state_key(record.audit_seq))
        payload = dict(doc.payload)
        payload["state"] = state
        payload["updated_at"] = self._clock.timestamp_iso()
        for key, value in changes.items():
            payload[key] = value
        stored = self._store.put(_state_key(record.audit_seq), payload)
        return self._to_record(stored.payload, stored.version)


__all__ = [
    "Outbox",
    "OutboxEntry",
    "DeliveryRecord",
    "DeliveryResult",
    "OUTBOX_STREAM",
    "QUEUED",
    "SENDING",
    "SENT",
    "DEAD",
    "DELIVERY_STATES",
]
