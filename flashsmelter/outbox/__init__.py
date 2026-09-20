"""关键事件外发（事务发件箱）。

本地动作与审计流水先落盘，关键事件信封同步进同一个持久化库的发件箱；后台中继
按顺序投递，断网垫存、恢复续送，对端靠 ``Idempotency-Key`` 去重，死信与对账
接口保证「送过的不重、没送的可见、送出的和本地对得上」。
"""

from __future__ import annotations

from .envelope import attach_audit_checksum, build_envelope, describe_envelope, fingerprint_of
from .outbox import (
    DEAD,
    QUEUED,
    SENDING,
    SENT,
    DeliveryRecord,
    DeliveryResult,
    Outbox,
    OutboxEntry,
)
from .relay import Relay
from .selector import DEFAULT_KEY_EVENTS, KeyEventSelector
from .transport import HttpTransport, Transport

__all__ = [
    "Outbox",
    "OutboxEntry",
    "DeliveryRecord",
    "DeliveryResult",
    "Relay",
    "KeyEventSelector",
    "DEFAULT_KEY_EVENTS",
    "HttpTransport",
    "Transport",
    "build_envelope",
    "attach_audit_checksum",
    "describe_envelope",
    "fingerprint_of",
    "QUEUED",
    "SENDING",
    "SENT",
    "DEAD",
]
