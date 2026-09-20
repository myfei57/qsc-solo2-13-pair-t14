"""关键事件外发。

设计目标对应现场的五句话：

* 「关键时刻自动往外送」——:class:`~flashsmelter.egress.policy.CriticalPolicy` 按
  动作白名单从审计流里挑关键事件，追加到发件箱流水；
* 「断网先垫着、恢复接着送」——发件箱是带序号与校验和的本地 JSONL，
  :class:`~flashsmelter.egress.service.EgressService` 按退避表补发；
* 「送过的别重发」——每个目标的发送状态机 ``pending → sent/dead`` 落盘，
  报文带确定性 ``event_id`` 与幂等头，对端可按号去重；
* 「没送到的要看得见」——status / events / attempts 同时暴露在 CLI、HTTP
  与指标里，pending、dead、下次重试时间、最后错误一览无余；
* 「送出去的和本地的记录对得上」——:meth:`EgressService.reconcile` 比对
  「审计关键事件 ↔ 发件箱 ↔ 各目标 sent 标记」的序号集合与载荷校验和。

整条链路复用平台的持久化原语（原子写、流水序号、逐行校验和），因此发件箱
本身也在 :command:`verify` 的完整性校验范围内。
"""

from __future__ import annotations

from .dispatcher import DeliveryResult, SinkPort, TargetDispatcher
from .policy import CRITICAL_ACTIONS, CriticalEvent, CriticalPolicy
from .pump import EgressPump
from .service import EgressService, OutboxEvent
from .sink import SinkError, WebhookSink

__all__ = [
    "CRITICAL_ACTIONS",
    "CriticalEvent",
    "CriticalPolicy",
    "DeliveryResult",
    "SinkPort",
    "TargetDispatcher",
    "EgressPump",
    "EgressService",
    "OutboxEvent",
    "SinkError",
    "WebhookSink",
]
