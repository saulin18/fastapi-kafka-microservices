from collections.abc import AsyncGenerator, Coroutine
import asyncio
from dataclasses import dataclass
from logging import getLogger

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from dishka import AsyncContainer, Provider, Scope, provide
from redis.asyncio import Redis

from .db import Database
from .kafka_producer import KafkaProducer
from .settings import Settings, get_settings

logger = getLogger(__name__)

_settings = get_settings()

celery_app = Celery(
    "independent_producer",
    broker=_settings.redis_url,
    backend=_settings.redis_url,
    include=["independent_producer.outbox_task"],
)
celery_app.conf.update(
    timezone="UTC",
    enable_utc=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    beat_schedule={
        "drain-outbox": {
            "task": "outbox_task.drain_outbox",
            "schedule": 1.0,
        },
    },
)

_worker_loop: asyncio.AbstractEventLoop | None = None
_worker_container: AsyncContainer | None = None


@dataclass(frozen=True, slots=True)
class MessagingRuntime:
    """Marker: background produce loop is running."""


class AppProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> Settings:
        return get_settings()

    @provide(scope=Scope.APP)
    def celery(self) -> Celery:
        return celery_app

    @provide(scope=Scope.APP)
    async def database(self, settings: Settings) -> AsyncGenerator[Database, None]:
        db = Database(settings.database_url)
        yield db
        await db.dispose()

    @provide(scope=Scope.APP)
    async def redis(self, settings: Settings) -> AsyncGenerator[Redis, None]:
        client = Redis.from_url(settings.redis_url)
        yield client
        await client.aclose()

    @provide(scope=Scope.APP)
    async def kafka_producer(
        self,
        settings: Settings,
        database: Database,
    ) -> AsyncGenerator[KafkaProducer, None]:
        producer = KafkaProducer(
            settings.kafka_url,
            database,
            topic=settings.kafka_topic,
        )
        await producer.start()
        yield producer
        await producer.stop()


class MessagingProvider(Provider):
    """FastAPI-only: starts the outbox enqueue loop. Not used in Celery workers."""

    @provide(scope=Scope.APP)
    async def messaging_runtime(
        self,
        kafka_producer: KafkaProducer,
    ) -> AsyncGenerator[MessagingRuntime, None]:
        produce_task = asyncio.create_task(
            kafka_producer.generate_events_and_publish(),
            name="kafka-producer",
        )
        try:
            yield MessagingRuntime()
        finally:
            produce_task.cancel()
            try:
                await produce_task
            except asyncio.CancelledError:
                pass


def get_worker_container() -> AsyncContainer:
    if _worker_container is None:
        raise RuntimeError("Celery worker container is not initialized")
    return _worker_container


def run_on_worker[T](coro: Coroutine[object, object, T]) -> T:
    if _worker_loop is None:
        raise RuntimeError("Celery worker event loop is not initialized")
    return _worker_loop.run_until_complete(coro)


@worker_process_init.connect
def _init_worker_dishka(**_kwargs) -> None:
    """Own AsyncContainer + one event loop (asyncpg/aiokafka need a stable loop)."""
    global _worker_loop, _worker_container
    from .container import create_worker_container

    _worker_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_worker_loop)
    _worker_container = create_worker_container()
   
    _worker_loop.run_until_complete(_worker_container.get(Database))
    _worker_loop.run_until_complete(_worker_container.get(KafkaProducer))
    logger.info("Celery worker AsyncContainer ready")


@worker_process_shutdown.connect
def _shutdown_worker_dishka(**_kwargs) -> None:
    global _worker_loop, _worker_container
    if _worker_container is not None and _worker_loop is not None:
        _worker_loop.run_until_complete(_worker_container.close())
    if _worker_loop is not None:
        _worker_loop.close()
    _worker_container = None
    _worker_loop = None
    logger.info("Celery worker AsyncContainer closed")
