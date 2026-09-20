from infrastructure.clock import utc_clock
from domain.transaction import Event
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from logging import getLogger
from uuid import uuid4

from dishka.integrations.fastapi import FromDishka, inject, setup_dishka
from fastapi import FastAPI

from domain.transaction import EventStatus
from infrastructure.logging import setup_logging
from infrastructure.messaging.kafka_producer import KafkaProducer
from ioc.container import create_container
from ioc.providers import MessagingRuntime
from settings import get_settings
logger = getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    setup_logging(settings.log_level)

    # Resolve APP graph: Redis, Kafka start, background loops
    await app.state.dishka_container.get(MessagingRuntime)

    logger.info("Application started")
    try:
        yield
    finally:
        await app.state.dishka_container.close()
        logger.info("Application stopped")
        
        
    


app = FastAPI(lifespan=lifespan)
setup_dishka(container=create_container(), app=app)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/transactions")
@inject
async def create_transaction(
    client_id: str,
    amount: float,
    producer: FromDishka[KafkaProducer],
    status: EventStatus = EventStatus.CREATED,
) -> dict[str, str]:
    event = Event(
        transaction_id=str(uuid4()),
        client_id=client_id,
        status=status,
        amount=amount,
        timestamp=utc_clock.now(),
    )
    await producer.publish(event)
    return {"transaction_id": event.transaction_id, "status": event.status.value}
