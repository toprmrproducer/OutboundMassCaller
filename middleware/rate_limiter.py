from collections import defaultdict
import time


class RateLimiter:
    def __init__(self):
        self._windows: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, key: str, max_requests: int, window_seconds: int) -> bool:
        now = time.time()
        window = self._windows[key]
        self._windows[key] = [t for t in window if now - t < window_seconds]
        if len(self._windows[key]) >= max_requests:
            return False
        self._windows[key].append(now)
        return True


rate_limiter = RateLimiter()
