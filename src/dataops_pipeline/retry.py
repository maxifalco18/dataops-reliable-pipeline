"""Retry with exponential backoff for transient failures (I/O, network, locks)."""

from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import TypeVar

from .logging_utils import get_logger

T = TypeVar("T")
log = get_logger(__name__)


def retry(
    attempts: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple[type[BaseException], ...] = (OSError,),
    sleep: Callable[[float], None] = time.sleep,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == attempts:
                        raise
                    delay = base_delay * 2 ** (attempt - 1)
                    log.warning(
                        "transient failure, retrying",
                        extra={"ctx": {"func": func.__name__, "attempt": attempt,
                                       "delay_s": delay, "error": str(exc)}},
                    )
                    sleep(delay)
            raise RuntimeError("unreachable")  # pragma: no cover

        return wrapper

    return decorator
