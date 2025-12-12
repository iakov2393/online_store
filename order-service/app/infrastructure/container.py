from typing import Callable

from dependency_injector import containers, providers
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.unit_of_work import UnitOfWork


class InfrastructureContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    async_engine = providers.Singleton[AsyncEngine](
        create_async_engine,
        config.db.dsn,
        pool_size=config.db.pool_size,
        pool_recycle=config.db.pool_recycle,
        future=True,
    )
    session_factory: Callable[..., AsyncSession] = providers.Factory(
        sessionmaker, async_engine, expire_on_commit=False, class_=AsyncSession
    )
    unit_of_work = providers.Singleton[UnitOfWork](
        UnitOfWork, session_factory=session_factory
    )
    kafka_producer = providers.Singleton[KafkaProducer](
        KafkaProducer,
        bootstrap_servers=config.kafka.bootstrap_servers,
        topic=config.kafka.topic,
    )
