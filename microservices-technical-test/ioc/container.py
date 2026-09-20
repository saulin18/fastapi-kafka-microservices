from dishka import AsyncContainer, make_async_container
from dishka.integrations.fastapi import FastapiProvider

from ioc.providers import AppProvider


def create_container() -> AsyncContainer:
    return make_async_container(AppProvider(), FastapiProvider())
