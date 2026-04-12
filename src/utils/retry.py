"""
retry.py — 외부 API/데이터 재시도 유틸

네트워크 장애·레이트 리밋 발생 시 지수 백오프(exponential backoff)로 자동 재시도.

사용법:
    # 데코레이터
    @with_retry(max_attempts=3, base_delay=2.0)
    def fetch_something():
        ...

    # 직접 호출 (결과가 None이면 최종 실패)
    result = retry_call(requests.get, url, max_attempts=3)
"""
from __future__ import annotations

import functools
import time
from typing import Any, Callable, TypeVar

from src.utils.logger import get_logger

logger = get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    exceptions: tuple = (Exception,),
    on_retry: Callable | None = None,
) -> Callable[[F], F]:
    """
    지수 백오프 재시도 데코레이터.

    Args:
        max_attempts: 최대 시도 횟수 (기본 3)
        base_delay:   첫 재시도 대기 시간(초)
        max_delay:    최대 대기 시간(초)
        exceptions:   재시도 대상 예외 튜플
        on_retry:     재시도 직전 호출할 콜백 (attempt, exc) → None
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt == max_attempts:
                        logger.warning(
                            f"[retry] {func.__name__} 최종 실패 "
                            f"({max_attempts}회): {type(e).__name__}: {e}"
                        )
                        raise
                    logger.warning(
                        f"[retry] {func.__name__} 실패 ({attempt}/{max_attempts}), "
                        f"{delay:.1f}초 후 재시도 — {type(e).__name__}: {e}"
                    )
                    if on_retry:
                        on_retry(attempt, e)
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
            raise last_exc  # unreachable, but for type checker
        return wrapper  # type: ignore[return-value]
    return decorator


def retry_call(
    func: Callable,
    *args,
    max_attempts: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    default: Any = None,
    **kwargs,
) -> Any:
    """
    함수를 재시도로 실행. 데코레이터 없이 일회성으로 사용.

    최종 실패 시 `default` 반환 (예외 전파 안 함).

    Example:
        data = retry_call(fdr.DataReader, ticker, start, end, max_attempts=3)
        if data is None:
            # 3번 모두 실패
    """
    delay = base_delay
    func_name = getattr(func, "__name__", str(func))
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_attempts:
                logger.warning(
                    f"[retry] {func_name} 최종 실패 ({max_attempts}회): {e}"
                )
                return default
            logger.warning(
                f"[retry] {func_name} 실패 ({attempt}/{max_attempts}), "
                f"{delay:.1f}초 후 재시도: {e}"
            )
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
    return default
