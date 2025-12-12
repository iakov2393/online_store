from typing import Awaitable, Callable
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.process_outbox_events import ProcessOutboxEventsUseCase
from app.core.models import EventTypeEnum, OutboxEvent, OutboxEventStatus
from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.repositories import OutboxRepository
from app.infrastructure.unit_of_work import UnitOfWork


@pytest.fixture
async def kafka_producer():
    """Mock Kafka producer for tests"""
    producer = AsyncMock(spec=KafkaProducer)
    producer.send_message = AsyncMock()

    # Mock context manager
    producer.__aenter__ = AsyncMock(return_value=producer)
    producer.__aexit__ = AsyncMock(return_value=None)

    return producer


@pytest.fixture
async def process_outbox_use_case(
    session_factory: async_sessionmaker[AsyncSession], kafka_producer: KafkaProducer
) -> ProcessOutboxEventsUseCase:
    uow = UnitOfWork(session_factory)
    return ProcessOutboxEventsUseCase(
        unit_of_work=uow, kafka_producer=kafka_producer, batch_size=100
    )


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


class TestProcessOutboxEventsUseCase:
    @pytest.mark.asyncio
    async def test_process_single_event_marks_as_sent(
        self,
        process_outbox_use_case: ProcessOutboxEventsUseCase,
        outbox_repo: OutboxRepository,
        outbox_event_factory: Callable[[dict], Awaitable[OutboxEvent]],
        kafka_producer: KafkaProducer,
        session: AsyncSession,
    ):
        # Given
        event = await outbox_event_factory({"order_id": "123", "user_id": "user_1"})
        event_id = event.id

        # When
        await process_outbox_use_case()

        # Then
        event = await outbox_repo.get_by_id(event_id)

        assert event is not None
        assert event.status == OutboxEventStatus.SENT

        # Check Kafka mock
        kafka_producer.send_message.assert_called_once()
        call_args = kafka_producer.send_message.call_args
        sent_message = call_args.kwargs["message"]
        assert sent_message["event_type"] == EventTypeEnum.ORDER_CREATED
        assert sent_message["payload"] == {"order_id": "123", "user_id": "user_1"}

    @pytest.mark.asyncio
    async def test_process_multiple_events_all_marked_as_sent(
        self,
        process_outbox_use_case: ProcessOutboxEventsUseCase,
        outbox_repo: OutboxRepository,
        outbox_event_factory: Callable[[dict], Awaitable[OutboxEvent]],
        kafka_producer: KafkaProducer,
        session: AsyncSession,
    ):
        # Given
        event_ids = []
        for i in range(3):
            event = await outbox_event_factory({"order_id": str(i)})
            event_ids.append(event.id)

        # When
        await process_outbox_use_case()

        # Then
        for event_id in event_ids:
            event = await outbox_repo.get_by_id(event_id)

            assert event is not None
            assert event.status == OutboxEventStatus.SENT

        # Check Kafka mock
        assert kafka_producer.send_message.call_count == 3
        all_calls = kafka_producer.send_message.call_args_list
        sent_order_ids = [
            call.kwargs["message"]["payload"]["order_id"] for call in all_calls
        ]
        assert set(sent_order_ids) == {"0", "1", "2"}

    @pytest.mark.asyncio
    async def test_get_pending_events_excludes_sent(
        self,
        process_outbox_use_case: ProcessOutboxEventsUseCase,
        outbox_repo: OutboxRepository,
        outbox_event_factory: Callable[[dict], Awaitable[OutboxEvent]],
        kafka_producer: KafkaProducer,
        session: AsyncSession,
    ):
        # Given
        event1 = await outbox_event_factory({"order_id": "1"})
        event2 = await outbox_event_factory({"order_id": "2"})

        # When - process once
        await process_outbox_use_case()

        # Then - check that both are marked as sent
        for event_id in [event1.id, event2.id]:
            event = await outbox_repo.get_by_id(event_id)
            assert event is not None
            assert event.status == OutboxEventStatus.SENT

        # Check that get_pending_events returns nothing
        new_events = await outbox_repo.get_pending_events()
        assert len(new_events) == 0

        # Check Kafka mock
        assert kafka_producer.send_message.call_count == 2

    @pytest.mark.asyncio
    async def test_respects_batch_size(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        outbox_event_factory: Callable[[dict], Awaitable[OutboxEvent]],
        outbox_repo: OutboxRepository,
        kafka_producer: KafkaProducer,
        session: AsyncSession,
    ):
        # Given
        batch_size = 2
        uow = UnitOfWork(session_factory)
        use_case = ProcessOutboxEventsUseCase(
            unit_of_work=uow, kafka_producer=kafka_producer, batch_size=batch_size
        )

        event_ids = []
        for i in range(5):
            event = await outbox_event_factory({"order_id": str(i)})
            event_ids.append(event.id)

        # When
        await use_case()

        # Then
        pending_events = await outbox_repo.get_pending_events()
        assert len(pending_events) == 3

        # Check Kafka mock - only batch_size events sent
        assert kafka_producer.send_message.call_count == batch_size
