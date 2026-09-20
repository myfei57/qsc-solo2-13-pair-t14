"""外发事件信封与对账指纹。

信封是本地审计记录与对端收到内容之间唯一的对照凭据：

* ``event_id`` 全局唯一，作为 HTTP ``Idempotency-Key``，对端据此去重，崩溃重投
  不会产生第二条业务事件；
* ``source``/``namespace`` 标明来自哪条产线，调度侧多产线汇聚时不会串；
* ``audit_seq`` + ``audit_checksum`` 指回本地审计流水的具体行，外发记录与本地
  记录因此可以逐行对账；
* ``payload_checksum`` 覆盖信封业务体，传输/存储被改动能被发现。
"""

from __future__ import annotations

import uuid
from typing import Any, Mapping

from ..audit import AuditEvent
from ..ns import Namespace
from ..runtime import Clock
from ..store.codec import canonical_json
from ..store import JournalEntry

ENVELOPE_VERSION = 1
EVENT_KIND = "flashsmelter.key-event"


def fingerprint_of(payload: Mapping[str, Any]) -> str:
    """业务体指纹：与落盘流水使用同一套规范化 JSON + sha256。"""

    import hashlib

    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def build_envelope(
    event: AuditEvent,
    *,
    namespace: Namespace,
    clock: Clock,
    event_id: str | None = None,
) -> dict[str, Any]:
    """把一条审计关键事件包成外发信封。"""

    body = {
        "kind": EVENT_KIND,
        "component": event.target.split("/", 1)[0] or event.target,
        "action": event.action,
        "target": event.target,
        "outcome": event.outcome,
        "actor": event.actor,
        "correlation_id": event.correlation_id,
        "occurred_at": event.at,
        "details": dict(event.details),
    }
    envelope = {
        "event_id": event_id or uuid.uuid4().hex,
        "schema_version": ENVELOPE_VERSION,
        "source": "flashsmelter",
        "namespace": namespace.prefix,
        "site": namespace.site,
        "unit": namespace.unit,
        "audit_seq": event.seq,
        "audit_checksum": "",  # 由 enqueue 时填入对应流水行的 checksum
        "emitted_at": clock.timestamp_iso(),
        "event": body,
    }
    envelope["payload_checksum"] = fingerprint_of(body)
    return envelope


def attach_audit_checksum(envelope: dict[str, Any], entry: JournalEntry) -> dict[str, Any]:
    """把审计流水行校验和补进信封并重算业务体外的整体指纹。"""

    envelope = dict(envelope)
    envelope["audit_checksum"] = entry.checksum
    envelope["payload_checksum"] = fingerprint_of(envelope["event"])
    return envelope


def describe_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """供状态查询的摘要，不重复携带完整 details。"""

    event = envelope.get("event", {})
    return {
        "event_id": str(envelope.get("event_id", "")),
        "audit_seq": envelope.get("audit_seq"),
        "namespace": str(envelope.get("namespace", "")),
        "component": str(event.get("component", "")),
        "action": str(event.get("action", "")),
        "target": str(event.get("target", "")),
        "outcome": str(event.get("outcome", "")),
        "occurred_at": event.get("occurred_at"),
        "payload_checksum": str(envelope.get("payload_checksum", "")),
    }


__all__ = ["build_envelope", "attach_audit_checksum", "describe_envelope", "fingerprint_of", "ENVELOPE_VERSION"]
