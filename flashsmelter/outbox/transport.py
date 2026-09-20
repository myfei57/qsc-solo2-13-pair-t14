"""外发传输。

只依赖标准库，与控制台的零依赖风格保持一致。传输层只负责「把信封送过去并解释
回执」，重试/退避/状态推进由 :class:`~flashsmelter.outbox.relay.Relay` 决定。

约定（上级系统/调度侧按此实现接收端）：

* ``POST <endpoint>``，``Content-Type: application/json``，信封整体为请求体；
* ``Idempotency-Key: <event_id>``，接收端对同一 key 必须只生效一次并返回同一回执；
* ``X-Audit-Seq`` 便于接收端按本地审计序号排序与缺口检测；
* ``2xx`` 视为已送达；``408/429/5xx`` 与网络异常视为暂时性失败、稍后重试；
  其余 ``4xx`` 视为永久性失败（载荷/鉴权问题，重试无意义），事件进死信。
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Mapping, Protocol, runtime_checkable

from ..runtime import Clock
from .outbox import DeliveryResult

RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


@runtime_checkable
class Transport(Protocol):
    def send(self, envelope: Mapping[str, Any]) -> DeliveryResult: ...


class HttpTransport:
    """通过 HTTP POST 投递事件信封。"""

    def __init__(
        self,
        endpoint: str,
        *,
        timeout_seconds: float = 5.0,
        clock: Clock | None = None,
    ) -> None:
        if not endpoint:
            raise ValueError("HttpTransport 需要非空 endpoint")
        self._endpoint = endpoint
        self._timeout = timeout_seconds
        self._clock = clock or Clock()

    def send(self, envelope: Mapping[str, Any]) -> DeliveryResult:
        body = json.dumps(dict(envelope), ensure_ascii=False, sort_keys=True).encode("utf-8")
        request = urllib.request.Request(
            self._endpoint,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Idempotency-Key": str(envelope["event_id"]),
                "X-Audit-Seq": str(envelope["audit_seq"]),
                "X-Source": str(envelope.get("namespace", "")),
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
                status = int(response.status)
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace") if exc.fp is not None else ""
            receipt = _parse_receipt(raw)
            if exc.code in RETRYABLE_STATUSES:
                return DeliveryResult("transient", status_code=exc.code, reason=f"http-{exc.code}")
            return DeliveryResult(
                "permanent",
                status_code=exc.code,
                reason=f"http-{exc.code}: {receipt.get('message', raw[:200])}",
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return DeliveryResult("transient", status_code=None, reason=f"network: {exc.reason if hasattr(exc, 'reason') else exc}")
        receipt = _parse_receipt(raw)
        # 走到这里状态码必为 2xx：其余响应 urlopen 已抛 HTTPError。
        return DeliveryResult(
            "ok",
            status_code=status,
            acknowledged_at=self._clock.timestamp_iso(),
            receiver_receipt=receipt,
        )


def _parse_receipt(raw: str) -> dict[str, Any]:
    if not raw.strip():
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw[:500]}
    return decoded if isinstance(decoded, dict) else {"data": decoded}


__all__ = ["Transport", "HttpTransport", "RETRYABLE_STATUSES"]
