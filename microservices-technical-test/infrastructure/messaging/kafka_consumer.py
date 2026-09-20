import json
from logging import getLogger

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from application.abstractions.idempotency.event_consumer import EventConsumer
from application.abstractions.idempotency.idempotent_store import IdempotentStore
from application.schemas.transaction_event import TransactionEventSchema
from domain.transaction import Event

logger = getLogger(__name__)


class KafkaConsumer(EventConsumer):
    def __init__(
        self,
        kafka_url: str,
        idempotency_store: IdempotentStore,
        *,
        topic: str = "transactions",
        group_id: str = "antifraude",
    ) -> None:
        self.idempotency_store = idempotency_store
        self.processed_events_count = 0
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka_url,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        )

    async def start(self) -> None:
        logger.info("Starting Kafka consumer")
        await self.consumer.start()
        logger.info("Kafka consumer started")

    async def stop(self) -> None:
        await self.consumer.stop()
        logger.info("Kafka consumer stopped")

    async def consume(self) -> None:
        async for message in self.consumer:
            try:
                schema = TransactionEventSchema.model_validate(message.value)
            except ValidationError:
                logger.exception("invalid event payload; skipping")
                logger.error(f"Invalid event payload: {message.value}")
                await self.consumer.commit()
                continue

            event = schema.to_domain()

            if await self.idempotency_store.check_if_exists(event.transaction_id):
                logger.info("Event %s already processed", event.transaction_id)
                await self.consumer.commit()
                continue

            await self.handle_event(event)
            await self.idempotency_store.mark_as_processed(event.transaction_id)

            self.processed_events_count += 1
            if self.processed_events_count % 10 == 0:
                await self.consumer.commit()
                logger.info("Committed after %s events", self.processed_events_count)

    async def handle_event(self, event: Event) -> None:
        logger.info("Handling event %s", event.transaction_id)
        # TODO: call antifraud / downstream
