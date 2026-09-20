import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from dishka import Provider, Scope, provide
from redis.asyncio import Redis

from application.abstractions.event_publisher import EventPublisher
from application.abstractions.idempotency.idempotent_store import IdempotentStore
from infrastructure.messaging.kafka_consumer import KafkaConsumer
from infrastructure.messaging.kafka_producer import KafkaProducer
from infrastructure.messaging.redis_store import RedisIdempotentStore
from settings import Settings, get_settings


@dataclass(frozen=True, slots=True)
class MessagingRuntime:
    """Marker: background produce/consume loops are running."""


class AppProvider(Provider):
    """Application-scoped wiring (settings, Redis, Kafka, workers)."""

    @provide(scope=Scope.APP)
    def settings(self) -> Settings:
        return get_settings()

    @provide(scope=Scope.APP)
    async def redis(self, settings: Settings) -> AsyncIterator[Redis]:
        client = Redis.from_url(settings.redis_url)
        yield client
        await client.aclose()

    @provide(scope=Scope.APP)
    def idempotent_store(
        self,
        redis: Redis,
        settings: Settings,
    ) -> IdempotentStore:
        return RedisIdempotentStore(
            redis,
            ttl_seconds=settings.idempotency_store_ttl,
        )

    @provide(scope=Scope.APP)
    async def kafka_producer(self, settings: Settings) -> AsyncIterator[KafkaProducer]:
        producer = KafkaProducer(
            settings.kafka_url,
            topic=settings.kafka_topic,
        )
        await producer.start()
        yield producer
        await producer.stop()

    @provide(scope=Scope.APP)
    def event_publisher(self, kafka_producer: KafkaProducer) -> EventPublisher:
        return kafka_producer

    @provide(scope=Scope.APP)
    async def kafka_consumer(
        self,
        settings: Settings,
        idempotent_store: IdempotentStore,
    ) -> AsyncIterator[KafkaConsumer]:
        consumer = KafkaConsumer(
            settings.kafka_url,
            idempotent_store,
            topic=settings.kafka_topic,
            group_id=settings.kafka_group_id,
        )
        await consumer.start()
        yield consumer
        await consumer.stop()

    @provide(scope=Scope.APP)
    async def messaging_runtime(
        self,
        kafka_producer: KafkaProducer,
        kafka_consumer: KafkaConsumer,
    ) -> AsyncIterator[MessagingRuntime]:
        consume_task = asyncio.create_task(
            kafka_consumer.consume(),
            name="kafka-consumer",
        )
        produce_task = asyncio.create_task(
            kafka_producer.generate_events_and_publish(),
            name="kafka-producer",
        )
        try:
            yield MessagingRuntime()
        finally:
            for task in (produce_task, consume_task):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
