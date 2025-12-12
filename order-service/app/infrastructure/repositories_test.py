import uuid
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.models import EventTypeEnum, Order, OrderStatusEnum, OutboxEventStatus
from app.infrastructure.repositories import OrderRepository, OutboxRepository


@pytest.fixture
async def order_repo(session: AsyncSession) -> OrderRepository:
    return OrderRepository(session)


class TestOrderRepository:
    @pytest.mark.asyncio
    async def test_create_order(self, order_repo: OrderRepository, item_factory):
        # Given
        items = [item_factory() for _ in range(3)]
        user_id = str(uuid.uuid4())

        # When
        order_new = await order_repo.create(
            OrderRepository.CreateDTO(
                user_id=user_id,
                items=items,
                amount=sum(item.price for item in items),
                status=OrderStatusEnum.NEW,
            )
        )

        # Then
        assert isinstance(order_new, Order)
        assert order_new.user_id == user_id
        assert order_new.items == items
        assert order_new.amount == Decimal("31.50")
        assert order_new.status == OrderStatusEnum.NEW
        assert [s.status for s in order_new.status_history] == [OrderStatusEnum.NEW]


class TestOutboxRepository:
    @pytest.mark.asyncio
    async def test_create_event(self, outbox_repo: OutboxRepository):
        # Given
        event_type = EventTypeEnum.ORDER_CREATED
        payload = {
            "order_id": "123e4567-e89b-12d3-a456-426614174000",
            "user_id": "user_123",
            "amount": 99.99,
        }

        # When
        event = await outbox_repo.create(
            OutboxRepository.CreateDTO(event_type=event_type, payload=payload)
        )

        # Then
        assert event.id is not None
        assert event.event_type == event_type
        assert event.payload == payload
        assert event.status == OutboxEventStatus.PENDING
        assert event.created_at is not None

    @pytest.mark.asyncio
    async def test_get_pending_events(self, outbox_repo: OutboxRepository):
        # Given
        await outbox_repo.create(
            OutboxRepository.CreateDTO(
                event_type=EventTypeEnum.ORDER_CREATED, payload={"order_id": "1"}
            )
        )

        # When
        events = await outbox_repo.get_pending_events()

        # Then
        assert len(events) == 1
        assert events[0].status == OutboxEventStatus.PENDING
        assert events[0].event_type == EventTypeEnum.ORDER_CREATED

    @pytest.mark.asyncio
    async def test_get_pending_events_with_limit(self, outbox_repo: OutboxRepository):
        # Given
        for i in range(5):
            await outbox_repo.create(
                OutboxRepository.CreateDTO(
                    event_type=EventTypeEnum.ORDER_CREATED, payload={"id": str(i)}
                )
            )

        # When
        events = await outbox_repo.get_pending_events(limit=3)

        # Then
        assert len(events) == 3

    @pytest.mark.asyncio
    async def test_mark_as_sent(self, outbox_repo: OutboxRepository):
        # Given
        event = await outbox_repo.create(
            OutboxRepository.CreateDTO(
                event_type=EventTypeEnum.ORDER_CREATED, payload={"order_id": "1"}
            )
        )

        # When
        await outbox_repo.mark_as_sent(event.id)

        # Then
        new_events = await outbox_repo.get_pending_events()
        assert len(new_events) == 0

    @pytest.mark.asyncio
    async def test_get_pending_events_excludes_sent(
        self, outbox_repo: OutboxRepository
    ):
        # Given
        event1 = await outbox_repo.create(
            OutboxRepository.CreateDTO(
                event_type=EventTypeEnum.ORDER_CREATED, payload={"order_id": "1"}
            )
        )
        event2 = await outbox_repo.create(
            OutboxRepository.CreateDTO(
                event_type=EventTypeEnum.ORDER_CREATED, payload={"order_id": "2"}
            )
        )
        await outbox_repo.mark_as_sent(event1.id)

        # When
        events = await outbox_repo.get_pending_events()

        # Then
        assert len(events) == 1
        assert events[0].id == event2.id
