from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class EventStatus(str, Enum):
    CREATED = "created"
    AUTHORIZED = "authorized"
    SETTLED = "settled"
    REJECTED = "rejected"
    

class TransactionEventStatus(str, Enum):
    CREATED = "created"
    AUTHORIZED = "authorized"
    SETTLED = "settled"
    REJECTED = "rejected"


@dataclass(frozen=True)
class Event:
    transaction_id: str
    client_id: str
    status: EventStatus
    amount: float
    timestamp: datetime
