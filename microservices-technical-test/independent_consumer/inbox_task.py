import time
import asyncio
import random
from datetime import datetime, timezone
from logging import getLogger

from sqlalchemy import select

from .db import Database
from .ioc import celery_app, get_worker_container, run_on_worker
from .persistence import Event, InboxMessage
from .schemas import TransactionEventSchema
logger = getLogger(__name__)
from concurrent.futures import ThreadPoolExecutor, as_completed

executor = ThreadPoolExecutor(max_workers=10)

BATCH_SIZE = 1000
# Just for the sake of testing, and in the real world we would use retries up to a certain limit, maybe a circuit breaker
# and a DLQ to handle the messages which exceed the retry limit.
ERROR_PROBABILITY = 0.05


def handle_event(event: Event) -> None:

    if random.random() < ERROR_PROBABILITY:
        raise RuntimeError("Failed to process event")
    logger.info(
        "Processed transaction_id=%s status=%s amount=%s",
        event.transaction_id,
        event.status.value,
        event.amount,
    )


def _process_one(row: InboxMessage) -> tuple[InboxMessage, str | None]:
    time.sleep(random.random() * 0.05)  # simulate work
    try:
        domain = TransactionEventSchema.model_validate(row.content).to_domain()
        handle_event(domain)
        return row, None
    except Exception as exc:
        logger.exception("Failed inbox id=%s", row.id)
        return row, str(exc)


async def _drain(database: Database) -> tuple[int, int]:
    errors = 0
    successes = 0
    now = datetime.now(tz=timezone.utc)

    async with database.session() as session:
        result = await session.scalars(
            select(InboxMessage)
            .where(InboxMessage.processed_on_utc.is_(None))
            .order_by(InboxMessage.received_on_utc)
            .limit(BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
        rows = list(result.all())
        if not rows:
            return 0, 0

        # Parallel business handling
        loop = asyncio.get_running_loop()

        async def one(row: InboxMessage) -> tuple[InboxMessage, str | None]:
            return await loop.run_in_executor(executor, _process_one, row)

        outcomes: list[tuple[InboxMessage, str | None]] = await asyncio.gather(*(one(row) for row in rows))
        
        # Sequential ORM updates on the locked rows (same transaction)
        for row, err in outcomes:
            row.processed_on_utc = now
            row.error = err
            if err:
                errors += 1
            else:
                successes += 1
        # session.commit() happens when exiting database.session()

    logger.info("Inbox drain errors=%s successes=%s", errors, successes)
    return errors, successes


@celery_app.task(name="inbox_task.drain_inbox")
def drain_inbox() -> tuple[int, int]:
    async def _run() -> tuple[int, int]:
        container = get_worker_container()
        database = await container.get(Database)
        return await _drain(database)

    return run_on_worker(_run())
