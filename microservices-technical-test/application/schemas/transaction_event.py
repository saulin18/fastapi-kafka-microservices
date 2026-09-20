from datetime import datetime
from pydantic import BaseModel, ConfigDict
from domain.transaction import Event, EventStatus

class TransactionEventSchema(BaseModel):
  
    model_config = ConfigDict(extra="forbid")

    transaction_id: str
    client_id: str
    status: EventStatus
    amount: float
    timestamp: datetime

    def to_domain(self) -> Event:
        return Event(
            transaction_id=self.transaction_id,
            client_id=self.client_id,
            status=EventStatus(self.status.value),
            amount=self.amount,
            timestamp=self.timestamp,
        )

    @classmethod
    def from_domain(cls, event: Event) -> "TransactionEventSchema":
        return cls(
            transaction_id=event.transaction_id,
            client_id=event.client_id,
            status=event.status,
            amount=event.amount,
            timestamp=event.timestamp,
        )