from abc import ABC, abstractmethod


class IdempotentStore(ABC):
    @abstractmethod
    async def check_if_exists(self, idempotency_key: str) -> bool:
        """Checks if the idempotency key exists."""
        pass

    @abstractmethod
    async def mark_as_processed(self, idempotency_key: str) -> None:
        """Marks the idempotency key as processed."""
        pass

    @abstractmethod
    async def unmark_as_processed(self, idempotency_key: str) -> None:
        """Unmarks the idempotency key as processed."""
        pass
