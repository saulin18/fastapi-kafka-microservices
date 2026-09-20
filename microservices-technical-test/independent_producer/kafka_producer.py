from datetime import datetime, timezone
import asyncio
import json
import uuid
from logging import getLogger
from random import choice, uniform

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, ConfigDict

from .db import Database
from .persistence import Event, EventStatus, Outbox

logger = getLogger(__name__)


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


class KafkaProducer:
    def __init__(
        self,
        kafka_url: str,
        database: Database,
        *,
        topic: str = "transactions",
    ) -> None:
        self._topic = topic
        self._database = database
        # Automatic batching: send() buffers per partition; linger + gzip do the rest
        self.producer = AIOKafkaProducer(
            bootstrap_servers=kafka_url,
            enable_idempotence=True,
            # probably do you want to use a lower number but it's ok for now
            acks="all",
            compression_type="gzip",
            linger_ms=10,
            value_serializer=lambda value: value.encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if key is not None else None,
        )

    async def start(self) -> None:
        logger.info("Starting Kafka producer")
        await self.producer.start()
        logger.info("Kafka producer started")

    async def stop(self) -> None:
        await self.producer.stop()
        logger.info("Kafka producer stopped")

    async def publish(self, event: Event) -> None:
        payload = TransactionEventSchema.from_domain(event).model_dump_json()
        await self.producer.send_and_wait(
            self._topic,
            value=payload,
            key=event.transaction_id,
        )
        logger.info("Published event %s", event.transaction_id)

    async def publish_payload(
        self, *, key: str, payload: dict, topic: str | None = None
    ) -> None:
        await self.producer.send_and_wait(
            topic or self._topic,
            value=json.dumps(payload),
            key=key,
        )
        logger.info("Published payload key=%s", key)

    async def publish_batch(
        self,
        items: list[tuple[str, dict, str | None]],
    ) -> list[bool]:
        """Queue with send(); aiokafka batches/compresses; then await all acks."""
        if not items:
            return []

        pending = []
        for key, payload, topic in items:
            fut: asyncio.Future = await self.producer.send(
                topic or self._topic,
                value=json.dumps(payload),
                key=key,
            )
            pending.append(fut)

        results = await asyncio.gather(*pending, return_exceptions=True)
        outcomes: list[bool] = []
        for (key, _payload, _topic), result in zip(items, results, strict=True):
            if isinstance(result, BaseException):
                logger.error("Failed to publish payload key=%s", key, exc_info=result)
                outcomes.append(False)
            else:
                outcomes.append(True)
        return outcomes

    async def enqueue_to_outbox(self, event: Event) -> None:
        now = datetime.now(tz=timezone.utc)
        async with self._database.session() as session:
            session.add(
                Outbox(
                    id=str(uuid.uuid4()),
                    type=self._topic,
                    payload={
                        "transaction_id": event.transaction_id,
                        "client_id": event.client_id,
                        "status": event.status.value,
                        "amount": event.amount,
                        "timestamp": event.timestamp.isoformat(),
                    },
                    aggregate_id=event.transaction_id,
                    aggregate_type="transaction",
                    correlation_id=event.transaction_id,
                    occurred_at=event.timestamp,
                    available_at=now,
                    trace_parent="",
                    attempts=0,
                )
            )
        logger.info("Enqueued outbox event %s", event.transaction_id)

    async def generate_events_and_publish(self) -> None:
        statuses = list(EventStatus)
        while True:
            event = Event(
                transaction_id=str(uuid.uuid4()),
                client_id=str(uuid.uuid4()),
                status=choice(statuses),
                amount=uniform(1, 1000),
                timestamp=datetime.now(tz=timezone.utc),
            )
            await self.enqueue_to_outbox(event)
            await asyncio.sleep(0.1)
