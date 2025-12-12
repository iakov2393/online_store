import asyncio
import json
from typing import Awaitable, Callable

import pytest
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.process_outbox_events import ProcessOutboxEventsUseCase
from app.core.models import EventTypeEnum, OutboxEvent, OutboxEventStatus
from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.repositories import OutboxRepository
from app.infrastructure.unit_of_work import UnitOfWork
from app.presentation.outbox_worker import OutboxWorker


@pytest.fixture
async def kafka_producer():
    """Real Kafka producer for e2e tests"""
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",  # Internal Docker network
        topic="orders-test",
    )
    await producer.start()
    yield producer
    await producer.stop()


@pytest.fixture
async def kafka_consumer():
    """Kafka consumer to verify messages were sent"""
    consumer = AIOKafkaConsumer(
        "orders-test",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id=f"test-group-{asyncio.get_event_loop().time()}",  # Unique group
        enable_auto_commit=False,
    )
    await consumer.start()
    # Seek to end to only read new messages
    consumer.seek_to_end()
    yield consumer
    await consumer.stop()


@pytest.fixture
async def process_outbox_use_case(
    session_factory: async_sessionmaker[AsyncSession], kafka_producer: KafkaProducer
) -> ProcessOutboxEventsUseCase:
    uow = UnitOfWork(session_factory)
    return ProcessOutboxEventsUseCase(
        unit_of_work=uow, kafka_producer=kafka_producer, batch_size=100
    )


@pytest.fixture
async def outbox_worker(
    process_outbox_use_case: ProcessOutboxEventsUseCase,
) -> OutboxWorker:
    return OutboxWorker(use_case=process_outbox_use_case)


@pytest.fixture
async def outbox_event_factory(
    session: AsyncSession,
) -> Callable[[dict], Awaitable[OutboxEvent]]:
    async def _(payload: dict) -> OutboxEvent:
        outbox_repo = OutboxRepository(session)
        event = await outbox_repo.create(
            OutboxRepository.CreateDTO(
                event_type=EventTypeEnum.ORDER_CREATED, payload=payload
            )
        )
        await session.commit()
        return event

    return _


@pytest.mark.e2e
class TestOutboxWorkerE2E:
    @pytest.mark.asyncio
    async def test_worker_processes_event_and_sends_to_kafka(
        self,
        outbox_worker: OutboxWorker,
        outbox_event_factory: Callable[[dict], Awaitable[OutboxEvent]],
        outbox_repo: OutboxRepository,
        kafka_consumer: AIOKafkaConsumer,
        session: AsyncSession,
    ):
        # Given
        event = await outbox_event_factory({"order_id": "123", "user_id": "user_1"})
        event_id = event.id

        # When - run worker for a short time
        worker_task = asyncio.create_task(outbox_worker.run())
        await asyncio.sleep(1)  # Let worker process events
        worker_task.cancel()

        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # Then - check event marked as sent in DB
        event = await outbox_repo.get_by_id(event_id=event_id)

        assert event.status == OutboxEventStatus.SENT

        # Check message in Kafka
        messages = []
        try:
            async with asyncio.timeout(3):
                async for msg in kafka_consumer:
                    messages.append(msg)
                    break
        except asyncio.TimeoutError:
            pass

        assert len(messages) >= 1
        message_value = json.loads(messages[0].value.decode("utf-8"))
        assert message_value["event_type"] == EventTypeEnum.ORDER_CREATED
        assert message_value["payload"]["order_id"] == "123"
        assert message_value["payload"]["user_id"] == "user_1"
