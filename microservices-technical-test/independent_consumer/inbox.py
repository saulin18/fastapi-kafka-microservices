from pydantic import ValidationError
from datetime import datetime, timezone
from logging import getLogger

from aiokafka import AIOKafkaConsumer
from sqlalchemy.dialects.postgresql import insert

from .db import Database
from .persistence import InboxMessage
from .schemas import TransactionEventSchema
from .settings import Settings

logger = getLogger(__name__)


async def run_inbox(
    consumer: AIOKafkaConsumer,
    database: Database,
    settings: Settings,
) -> None:
    async for message in consumer:
        try:
            event = TransactionEventSchema.model_validate_json(message.value or "{}")
        except (ValidationError, ValueError, TypeError):
            logger.exception(
                "Bad payload topic=%s offset=%s — skipping",
                message.topic,
                message.offset,
            )
            await consumer.commit()
            continue

        message_id = message.key or event.transaction_id
        now = datetime.now(tz=timezone.utc)

        stmt = (
            insert(InboxMessage)
            .values(
                id=str(message_id),
                type=message.topic or settings.kafka_topic,
                content=event.model_dump(mode="json"),
                received_on_utc=now,
            )
            .on_conflict_do_nothing(index_elements=[InboxMessage.id])
        )

        async with database.session() as session:
            await session.execute(stmt)

        await consumer.commit()
        logger.debug("Inbox stored id=%s", message_id)
