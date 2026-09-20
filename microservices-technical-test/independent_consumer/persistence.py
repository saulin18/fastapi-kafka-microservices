from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, Index, String, Text, func, text
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


class InboxMessage(Base):


    __tablename__ = "inbox_messages"
    __table_args__ = (
        Index(
            "idx_inbox_messages_unprocessed",
            "received_on_utc",
            "processed_on_utc",
            postgresql_include=["id", "type", "content"],
            postgresql_where=text("processed_on_utc IS NULL"),
        ),
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    type: Mapped[str] = mapped_column(String(255), nullable=False)
    content: Mapped[dict] = mapped_column(JSONB, nullable=False)
    received_on_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    processed_on_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
