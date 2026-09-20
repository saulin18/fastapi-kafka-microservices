from datetime import datetime, timezone

from application.abstractions.clock import Clock


class SystemClock:
    """Production clock: always timezone-aware UTC."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FixedClock:
    """Test clock: returns a fixed instant."""

    def __init__(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=timezone.utc)
        self._instant = instant

    def now(self) -> datetime:
        return self._instant


utc_clock: Clock = SystemClock()
