"""关键事件策略：从审计事件里挑出「上级系统和调度要看的关键时刻」。

默认只挑会改变炉况或代表联锁动作的少数动作（点火/喷吹/放渣放铜/联锁跳车/
转炉关键节点），门控拒绝与失败同样外发——调度不仅要知道「做了」，也要
知道「被挡住了」。白名单可通过配置覆盖，避免关键时刻清单散落在代码各处。

审计流里的动作名是不带组件前缀的裸名（如 ``feed``），而裸名在组件之间会
撞（furnace/conc 都有 ``stop``），因此策略统一用审计事件 ``target`` 的首段
（组件名）还原成 ``furnace.feed`` 这种限定名再匹配。
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

# 默认关键动作目录：组件.动作，支持配置里用 ``furnace.*`` 这类通配。
CRITICAL_ACTIONS: frozenset[str] = frozenset(
    {
        "burner.ignite",
        "burner.confirm_flame",
        "burner.trip",
        "oxygen.establish",
        "oxygen.rollback",
        "conc.arm",
        "conc.inject",
        "conc.stop",
        "furnace.start",
        "furnace.feed",
        "furnace.tap",
        "furnace.stop",
        "furnace.latch",
        "furnace.reset",
        "settler.begin_tap",
        "settler.end_tap",
        "slag.tap",
        "matte.tap",
        "conv.charge",
        "conv.blow",
        "conv.discharge",
        "conv.finish_batch",
        "waste.cooldown",
    }
)

# 事件分级：失败/拒绝要在标题层面就能被对端筛出来。
_SEVERITY_BY_OUTCOME = {"ok": "info", "rejected": "warning", "failed": "critical"}


@dataclass(frozen=True, slots=True)
class CriticalEvent:
    """一条待外发关键事件的完整视图。

    ``event_id`` 由命名空间与审计流水序号确定性派生：同一条审计事件无论
    重启多少次、补发多少轮，号码都不变——这是端到端幂等的锚点。
    """

    event_id: str
    audit_seq: int
    at: str
    namespace: str
    component: str
    action: str
    target: str
    outcome: str
    severity: str
    actor: str
    correlation_id: str
    details: Mapping[str, Any]
    payload_checksum: str

    def envelope(self) -> dict[str, Any]:
        """外发报文：字段稳定，``payload_checksum`` 供对端回查本地记录。"""

        return {
            "event_id": self.event_id,
            "at": self.at,
            "namespace": self.namespace,
            "component": self.component,
            "action": self.action,
            "target": self.target,
            "outcome": self.outcome,
            "severity": self.severity,
            "actor": self.actor,
            "correlation_id": self.correlation_id,
            "details": dict(self.details),
            "payload_checksum": self.payload_checksum,
        }


def event_id_for(namespace: str, audit_seq: int) -> str:
    return f"{namespace}:critical:{audit_seq}"


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def payload_checksum_of(envelope_body: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(envelope_body).encode("utf-8")).hexdigest()


def qualify_action(action: str, target: str) -> str:
    """``feed`` + ``furnace/H-1`` → ``furnace.feed``。"""

    component = (target or "").split("/", 1)[0].strip()
    if not component:
        return action
    return f"{component}.{action}"


class CriticalPolicy:
    """判定一条审计事件是否关键，并把它翻译成外发事件。"""

    def __init__(self, namespace: str, *, patterns: frozenset[str] | None = None) -> None:
        self._namespace = namespace
        self._patterns = tuple(sorted(CRITICAL_ACTIONS if patterns is None else patterns))

    @property
    def patterns(self) -> tuple[str, ...]:
        return self._patterns

    def is_critical_action(self, qualified: str, bare: str) -> bool:
        for pattern in self._patterns:
            if qualified == pattern or fnmatch.fnmatchcase(qualified, pattern):
                return True
            if "." not in pattern and (bare == pattern or fnmatch.fnmatchcase(bare, pattern)):
                return True
        return False

    def from_audit(self, event: Mapping[str, Any]) -> CriticalEvent | None:
        bare = str(event.get("action", ""))
        target = str(event.get("target", ""))
        qualified = qualify_action(bare, target)
        if not self.is_critical_action(qualified, bare):
            return None
        outcome = str(event.get("outcome", "ok"))
        audit_seq = int(event["seq"])
        component = qualified.split(".", 1)[0]
        body = {
            "at": str(event.get("at", "")),
            "namespace": str(event.get("namespace", self._namespace)),
            "component": component,
            "action": qualified,
            "target": target,
            "outcome": outcome,
            "actor": str(event.get("actor", "unknown")),
            "correlation_id": str(event.get("correlation_id", "")),
            "details": dict(event.get("details") or {}),
        }
        return CriticalEvent(
            event_id=event_id_for(self._namespace, audit_seq),
            audit_seq=audit_seq,
            at=body["at"],
            namespace=body["namespace"],
            component=component,
            action=qualified,
            target=target,
            outcome=outcome,
            severity=_SEVERITY_BY_OUTCOME.get(outcome, "info"),
            actor=body["actor"],
            correlation_id=body["correlation_id"],
            details=body["details"],
            payload_checksum=payload_checksum_of(body),
        )


__all__ = [
    "CRITICAL_ACTIONS",
    "CriticalEvent",
    "CriticalPolicy",
    "event_id_for",
    "payload_checksum_of",
    "qualify_action",
    "canonical_json",
]
