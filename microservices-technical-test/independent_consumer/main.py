from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from logging import getLogger

from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI

from .container import create_container
from .ioc import MessagingRuntime

logger = getLogger(__name__)

container = create_container()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await app.state.dishka_container.get(MessagingRuntime)
    try:
        yield
    finally:
        await app.state.dishka_container.close()
        logger.info("Consumer stopped")


app = FastAPI(title="independent-consumer", lifespan=lifespan)
setup_dishka(container=container, app=app)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "consumer"}
