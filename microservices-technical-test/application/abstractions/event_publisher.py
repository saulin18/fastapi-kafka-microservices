from abc import ABC, abstractmethod

from domain.transaction import Event


class EventPublisher(ABC):
    @abstractmethod
    async def publish(self, event: Event) -> None:
        """Publish a domain event to the message broker."""
        ...
