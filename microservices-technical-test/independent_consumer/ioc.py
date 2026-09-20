import asyncio
from collections.abc import AsyncGenerator, Coroutine
from dataclasses import dataclass
from logging import getLogger

from aiokafka import AIOKafkaConsumer
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from dishka import AsyncContainer, Provider, Scope, provide

from .db import Database
from .inbox import run_inbox
from .settings import Settings, get_settings

logger = getLogger(__name__)

_settings = get_settings()

celery_app = Celery(
    "independent_consumer",
    broker=_settings.redis_url,
    backend=_settings.redis_url,
    include=["independent_consumer.inbox_task"],
)
celery_app.conf.update(
    timezone="UTC",
    enable_utc=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    beat_schedule={
        "drain-inbox": {
            "task": "inbox_task.drain_inbox",
            "schedule": 1.0,
        },
    },
)

_worker_loop: asyncio.AbstractEventLoop | None = None
_worker_container: AsyncContainer | None = None


@dataclass(frozen=True, slots=True)
class MessagingRuntime:
    """Marker: Kafka→inbox loop is running."""


class AppProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> Settings:
        return get_settings()

    @provide(scope=Scope.APP)
    def celery(self) -> Celery:
        return celery_app

    @provide(scope=Scope.APP)
    async def database(self, settings: Settings) -> AsyncGenerator[Database]:
        db = Database(settings.database_url)
        yield db
        await db.dispose()

    @provide(scope=Scope.APP)
    async def kafka_consumer(
        self,
        settings: Settings,
    ) -> AsyncGenerator[AIOKafkaConsumer]:
        consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_url,
            group_id=settings.kafka_group_id,
            enable_auto_commit=False,
            value_deserializer=lambda value: value.decode("utf-8"),
            key_deserializer=lambda key: (
                key.decode("utf-8") if key is not None else None
            ),
        )
        await consumer.start()
        yield consumer
        await consumer.stop()


class MessagingProvider(Provider):
    @provide(scope=Scope.APP)
    async def messaging_runtime(
        self,
        kafka_consumer: AIOKafkaConsumer,
        database: Database,
        settings: Settings,
    ) -> AsyncGenerator[MessagingRuntime]:
        consume_task = asyncio.create_task(
            run_inbox(kafka_consumer, database, settings),
            name="kafka-inbox",
        )
        try:
            yield MessagingRuntime()
        finally:
            consume_task.cancel()
            try:
                await consume_task
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
    global _worker_loop, _worker_container
    from .container import create_worker_container

    _worker_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_worker_loop)
    # Worker: Database only (no Kafka inbox loop / MessagingRuntime)
    _worker_container = create_worker_container()
    _worker_loop.run_until_complete(_worker_container.get(Database))
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
