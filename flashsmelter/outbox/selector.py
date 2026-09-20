"""关键事件选择。

「关键时刻」的口径必须显式、可配置，而不是把所有审计行一股脑外发。选择规则形如：

* ``furnace.feed``         —— 精确匹配某个动作，只外发成功的一次；
* ``furnace.*``            —— 匹配某个组件的全部动作（只外发成功）；
* ``furnace.latch:rejected`` —— 也关注被联锁挡住的尝试（跳车/闩锁类尤其要看拒绝）；
* ``*``                    —— 匹配所有动作；
* ``*:failed``             —— 所有未捕获失败都外发。

缺省目录覆盖冶炼关键时刻：开停炉、联锁闩锁/跳车、富氧建立与爬坡、精矿喷吹、
放渣放铜、转炉装包吹炼、余热锅炉启动与冷却。配置为空时使用该缺省目录。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

DEFAULT_KEY_EVENTS: tuple[str, ...] = (
    "furnace.start",
    "furnace.feed",
    "furnace.tap",
    "furnace.stop",
    "furnace.latch:ok",
    "furnace.latch:rejected",
    "furnace.reset",
    "burner.ignite",
    "burner.trip:ok",
    "burner.trip:rejected",
    "burner.reset",
    "oxygen.establish",
    "oxygen.ramp",
    "oxygen.rollback",
    "conc.arm",
    "conc.inject",
    "conc.stop",
    "settler.begin_tap",
    "settler.end_tap",
    "slag.tap",
    "matte.tap",
    "conv.charge",
    "conv.blow",
    "conv.finish_batch",
    "waste.start",
    "waste.cooldown",
    "waste.finish_cooling",
)


@dataclass(frozen=True, slots=True)
class _Rule:
    component: str  # "" 表示通配所有组件
    action: str  # "" 表示通配组件内全部动作
    outcome: str  # "" 表示仅成功（ok）

    def matches(self, action: str, target: str, outcome: str) -> bool:
        if "." in action:
            component, _, verb = action.partition(".")
        else:
            # target 可能带业务标识，如 "furnace/H-1"：组件名只取首段。
            component = target.split("/", 1)[0] or target
            verb = action
        if self.component and self.component != component:
            return False
        if self.action and self.action != verb:
            return False
        wanted = self.outcome or "ok"
        return wanted == outcome


def _parse_rule(pattern: str) -> _Rule:
    text = pattern
    outcome = ""
    for suffix in (":ok", ":rejected", ":failed"):
        if text.endswith(suffix):
            text, outcome = text[: -len(suffix)], suffix[1:]
            break
    if text == "*":
        return _Rule(component="", action="", outcome=outcome)
    if text.endswith(".*"):
        return _Rule(component=text[:-2], action="", outcome=outcome)
    component, _, verb = text.partition(".")
    return _Rule(component=component, action=verb, outcome=outcome)


class KeyEventSelector:
    """按显式规则目录判定一条审计事件是否属于需要外发的关键事件。"""

    def __init__(self, patterns: Iterable[str] | None = None) -> None:
        chosen = tuple(patterns) if patterns else DEFAULT_KEY_EVENTS
        self._patterns = chosen
        self._rules = tuple(_parse_rule(pattern) for pattern in chosen)

    @property
    def patterns(self) -> tuple[str, ...]:
        return self._patterns

    def is_key_event(self, event: Any) -> bool:
        if hasattr(event, "to_dict"):
            event = event.to_dict()
        action = str(event.get("action", ""))
        target = str(event.get("target", ""))
        outcome = str(event.get("outcome", "ok"))
        return any(rule.matches(action, target, outcome) for rule in self._rules)


__all__ = ["KeyEventSelector", "DEFAULT_KEY_EVENTS"]
