"""外发中继：后台扫描、投递、退避与崩溃恢复。

中继是唯一驱动发件箱状态前进的地方，循环做三件事：

1. ``ingest_audit`` 增量扫描审计流水，把没走进即时入箱路径的关键事件补齐；
2. ``due`` 按顺序取出到期事件，置 ``sending`` 后调用传输层；
3. 成功置 ``sent``（对端回执落盘）；暂时性失败按指数退避回到 ``queued``；
   永久性失败或超过最大次数置 ``dead``，等人处理，绝不丢事件。

严格顺序投递：遇到一条暂时性失败就结束本轮，先垫着的老事件排在最前；网络恢复
后下一轮从它接着送。指数退避封顶，避免长时间断网时打爆对端。
"""

from __future__ import annotations

import logging
import threading

from ..audit import AUDIT_STREAM
from ..runtime import Clock
from .outbox import DeliveryResult, Outbox
from .transport import Transport

LOGGER = logging.getLogger("flashsmelter.outbox")


class Relay:
    """把发件箱里的关键事件按顺序送出去。"""

    def __init__(
        self,
        outbox: Outbox,
        transport: Transport | None,
        *,
        clock: Clock,
        metrics=None,
        poll_interval_seconds: float = 2.0,
        backoff_seconds: float = 5.0,
        backoff_max_seconds: float = 300.0,
        ingest_batch: int = 500,
    ) -> None:
        self._outbox = outbox
        self._transport = transport
        self._clock = clock
        self._metrics = metrics
        self._poll_interval = poll_interval_seconds
        self._backoff = backoff_seconds
        self._backoff_max = backoff_max_seconds
        self._ingest_batch = ingest_batch
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._wake = threading.Event()

    # ------------------------------------------------------------- 生命周期
    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="flashsmelter-outbox", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def kick(self) -> None:
        """动作刚入箱时唤醒一轮，把正常路径的延迟压到最低。"""

        self._wake.set()

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def delivery_enabled(self) -> bool:
        """未配置投递端点时只做本地留存，不触网。"""

        return self._transport is not None

    # ------------------------------------------------------------- 单轮投递
    def run_once(self) -> dict[str, int]:
        """执行一轮补齐 + 投递。供后台循环与手动 ``outbox flush`` 共用。

        返回本轮计数；传输层未配置时只做审计补齐，不触网。
        """

        ingested = self._outbox.ingest_audit(AUDIT_STREAM, batch_limit=self._ingest_batch)
        counts = {"ingested": ingested, "sent": 0, "retried": 0, "dead": 0, "attempts": 0}
        if self._transport is None:
            return counts
        due = self._outbox.due()
        for entry, record in due:
            claimed = self._outbox.claim(record)
            try:
                result = self._transport.send(entry.envelope)
            except Exception as exc:  # 传输层自身异常按暂时性失败处理，事件不丢
                LOGGER.warning("外发传输抛出异常，按暂时性失败重试", exc_info=True)
                result = DeliveryResult("transient", reason=f"transport-error: {exc}")
            counts["attempts"] += 1
            self._count("outbox.attempts")
            if result.ok:
                self._outbox.mark_sent(claimed, result)
                counts["sent"] += 1
                self._count("outbox.sent")
                continue
            if result.transient:
                backoff = self._backoff_for(claimed.attempts + 1)
                self._outbox.mark_retry(claimed, result, backoff_seconds=backoff)
                counts["retried"] += 1
                self._count("outbox.retried")
                break  # 保持顺序：最老的没送达，不越过它送后面的
            self._outbox.mark_dead(claimed, result)
            counts["dead"] += 1
            self._count("outbox.dead")
            LOGGER.error(
                "关键事件外发永久性失败，进入死信：audit_seq=%s action=%s status=%s",
                claimed.audit_seq,
                entry.envelope.get("event", {}).get("action"),
                result.status_code,
            )
        return counts

    def _backoff_for(self, attempt: int) -> float:
        # 指数退避：base * 2^(n-1)，封顶；attempt=1 → base。
        doubled = self._backoff * (2 ** max(attempt - 1, 0))
        return min(doubled, self._backoff_max)

    # ------------------------------------------------------------- 后台循环
    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:  # 后台循环绝不因单轮异常退出
                LOGGER.exception("外发中继本轮执行失败，下轮继续")
            self._wake.clear()
            self._wake.wait(timeout=self._poll_interval)

    def _count(self, name: str) -> None:
        if self._metrics is not None:
            self._metrics.inc(name)


__all__ = ["Relay"]
