from datetime import datetime, timezone
from logging import getLogger

from sqlalchemy import select

from .db import Database
from .ioc import celery_app, get_worker_container, run_on_worker
from .kafka_producer import KafkaProducer
from .persistence import Outbox

logger = getLogger(__name__)


async def _drain(database: Database, producer: KafkaProducer) -> int:
    published = 0
    now = datetime.now(tz=timezone.utc)

    async with database.session() as session:
        result = await session.scalars(
            select(Outbox)
            .where(Outbox.processed_at.is_(None))
            .where(Outbox.available_at <= now)
            .order_by(Outbox.available_at, Outbox.occurred_at)
            .limit(50)
            .with_for_update(skip_locked=True)
        )
        rows = list(result.all())

        outcomes = await producer.publish_batch(
            [(row.aggregate_id, row.payload, row.type) for row in rows]
        )

        for row, ok in zip(rows, outcomes, strict=True):
            if ok:
                row.processed_at = datetime.now(tz=timezone.utc)
                published += 1
            else:
                row.attempts += 1

    logger.info("Outbox drain published=%s", published)
    return published


@celery_app.task(name="outbox_task.drain_outbox")
def drain_outbox() -> int:
    async def _run() -> int:
        container = get_worker_container()
        database = await container.get(Database)
        producer = await container.get(KafkaProducer)
        return await _drain(database, producer)

    return run_on_worker(_run())
