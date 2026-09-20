from dishka import AsyncContainer, make_async_container
from dishka.integrations.fastapi import FastapiProvider

from .ioc import AppProvider, MessagingProvider


def create_container() -> AsyncContainer:
    """FastAPI process: infra + event generator + FastAPI integration."""
    return make_async_container(
        AppProvider(),
        MessagingProvider(),
        FastapiProvider(),
    )


def create_worker_container() -> AsyncContainer:
    """Celery worker: infra only (no MessagingRuntime / no FastAPI)."""
    return make_async_container(AppProvider())
