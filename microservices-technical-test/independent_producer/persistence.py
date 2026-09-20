from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import uuid4

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class EventStatus(str, Enum):
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


class Outbox(Base):
    __tablename__ = "outbox"

    id: Mapped[str] = mapped_column(String(255), primary_key=True, default=lambda: str(uuid4()))
    type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    aggregate_id: Mapped[str] = mapped_column(String(255), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(255), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(255), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    trace_parent: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
