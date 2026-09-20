"""外发通道：把报文送出去，并把对端应答翻译成确定的投递结果。

只有一个内置实现 :class:`WebhookSink`（上级系统/调度收事件的 HTTP 入口）。
传输层刻意做薄：重试、退避、状态机都在外层，sink 只回答三件事——
成功（可带回对端流水号）、可重试失败（断网、超时、5xx）、永久失败
（4xx，除 408/425/429 外）。
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, runtime_checkable

from ..errors import ValidationError

# 这些 4xx 语义上是「稍后再试」，不能判死刑。
_RETRYABLE_STATUS = frozenset({408, 425, 429})


class SinkError(Exception):
    """发送失败；``permanent=True`` 表示再发多少次都不会成功。"""

    def __init__(self, message: str, *, permanent: bool = False, status: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.permanent = permanent
        self.status = status


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    ok: bool
    permanent: bool
    status: int | None
    remote_id: str | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "permanent": self.permanent,
            "status": self.status,
            "remote_id": self.remote_id,
            "detail": self.detail,
        }


@runtime_checkable
class SinkPort(Protocol):
    """外发通道协议，测试用内存实现替换即可覆盖断网/恢复场景。"""

    name: str

    def deliver(self, envelope: Mapping[str, Any], *, timeout: float) -> DeliveryResult: ...


class WebhookSink:
    """HTTP POST + 幂等头的 webhook 通道。"""

    name = "webhook"

    def __init__(self, endpoint: str, *, token: str | None = None, extra_headers: Mapping[str, str] | None = None) -> None:
        if not endpoint.startswith(("http://", "https://")):
            raise ValidationError(
                "外发端点必须是 http(s) URL", details={"endpoint": endpoint}
            )
        self.endpoint = endpoint
        self._token = token
        self._extra_headers = dict(extra_headers or {})

    def deliver(self, envelope: Mapping[str, Any], *, timeout: float) -> DeliveryResult:
        event_id = str(envelope.get("event_id", ""))
        body = json.dumps(envelope, ensure_ascii=False, sort_keys=True).encode("utf-8")
        request = urllib.request.Request(self.endpoint, data=body, method="POST")
        request.add_header("Content-Type", "application/json; charset=utf-8")
        request.add_header("X-Event-Id", event_id)
        request.add_header("Idempotency-Key", event_id)
        if self._token:
            request.add_header("Authorization", f"Bearer {self._token}")
        for name, value in self._extra_headers.items():
            request.add_header(name, value)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = int(response.status)
                remote_id = self._extract_remote_id(response)
                return DeliveryResult(
                    ok=200 <= status < 300,
                    permanent=False,
                    status=status,
                    remote_id=remote_id,
                    detail="accepted",
                )
        except urllib.error.HTTPError as exc:
            permanent = 400 <= exc.code < 500 and exc.code not in _RETRYABLE_STATUS
            return DeliveryResult(
                ok=False,
                permanent=permanent,
                status=exc.code,
                remote_id=None,
                detail=f"HTTP {exc.code}",
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            return DeliveryResult(
                ok=False,
                permanent=False,
                status=None,
                remote_id=None,
                detail=f"连接失败: {reason}",
            )

    @staticmethod
    def _extract_remote_id(response: Any) -> str | None:
        """对端最好回 ``X-Event-Id`` 或 JSON ``event_id`` 作为收讫凭证。"""

        header = response.headers.get("X-Event-Id")
        if header:
            return header
        try:
            parsed = json.loads(response.read().decode("utf-8"))
        except (ValueError, UnicodeDecodeError, OSError):
            return None
        if isinstance(parsed, dict):
            value = parsed.get("event_id") or parsed.get("id")
            return None if value is None else str(value)
        return None


__all__ = ["WebhookSink", "SinkError", "DeliveryResult", "SinkPort"]
