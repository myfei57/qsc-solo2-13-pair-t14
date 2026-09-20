"""后台补发泵。

``serve`` 长驻时由一个守护线程按固定间隔调用 :meth:`EgressService.pump_once`：
网络正常时新关键事件下一轮就发出去；断网期间事件在发件箱里垫着，每轮只做
一次注定失败的尝试后按退避表等待；网络恢复后待发事件自动续送，无需人工
补录。工艺动作线程绝不阻塞在泵上——泵里任何异常都只计数、记录，不向上抛。
"""

from __future__ import annotations

import threading
from typing import Any

LOGGER_NAME = "flashsmelter.egress"


class EgressPump:
    def __init__(self, service: Any, *, interval_seconds: float, logger: Any = None) -> None:
        if interval_seconds <= 0:
            raise ValueError("外发泵间隔必须为正数")
        self._service = service
        self._interval = float(interval_seconds)
        self._logger = logger
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="flashsmelter-egress-pump", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._service.pump_once()
            except Exception as exc:  # 泵绝不能把进程带崩
                if self._logger is not None:
                    self._logger.warning("外发泵本轮失败: %s", exc)
            self._stop.wait(self._interval)


__all__ = ["EgressPump"]
