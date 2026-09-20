from abc import ABC, abstractmethod
from domain.transaction import Event

class EventConsumer(ABC):
    @abstractmethod
    async def consume(self) -> None:
        """
        Start consuming events from the Kafka topic.
        """
        pass