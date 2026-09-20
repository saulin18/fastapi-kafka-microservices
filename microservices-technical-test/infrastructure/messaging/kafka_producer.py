import asyncio
import uuid
from logging import getLogger
from random import choice, uniform

from aiokafka import AIOKafkaProducer

from application.abstractions.event_publisher import EventPublisher
from application.schemas.transaction_event import TransactionEventSchema
from domain.transaction import Event, EventStatus
from infrastructure.clock import utc_clock

logger = getLogger(__name__)


class KafkaProducer(EventPublisher):
    def __init__(self, kafka_url: str, *, topic: str = "transactions") -> None:
        self._topic = topic
        self.producer = AIOKafkaProducer(
            bootstrap_servers=kafka_url,
            enable_idempotence=True,
            acks="all",
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

    async def generate_events_and_publish(self) -> None:
        """Continuously generate mock events and publish them to Kafka."""
        statuses = list(EventStatus)
        while True:
            event = Event(
                transaction_id=str(uuid.uuid4()),
                client_id=str(uuid.uuid4()),
                status=choice(statuses),
                amount=uniform(1, 1000),
                timestamp=utc_clock.now(),
            )
            await self.publish(event)
            await asyncio.sleep(0.1)
