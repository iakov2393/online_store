import uuid
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.create_order import CreateOrderUseCase, OrderDTO
from app.core.models import EventTypeEnum, Order, OrderStatusEnum, OutboxEventStatus
from app.infrastructure.repositories import OutboxRepository
from app.infrastructure.unit_of_work import UnitOfWork


@pytest.fixture
async def create_order_use_case(
    session_factory: async_sessionmaker[AsyncSession],
) -> CreateOrderUseCase:
    uow = UnitOfWork(session_factory)
    return CreateOrderUseCase(unit_of_work=uow)


class TestCreateOrderUseCase:
    @pytest.mark.asyncio
    async def test_create_order_success(
        self,
        create_order_use_case: CreateOrderUseCase,
        outbox_repo: OutboxRepository,
        item_factory,
    ):
        # Given
        items = [item_factory() for _ in range(3)]
        user_id = str(uuid.uuid4())
        order_dto = OrderDTO(user_id=user_id, items=items)

        # When
        order = await create_order_use_case(order_dto)

        # Then
        assert isinstance(order, Order)
        assert order.id is not None
        assert order.user_id == user_id
        assert order.items == items
        assert order.amount == Decimal("31.50")
        assert order.status == OrderStatusEnum.NEW

    @pytest.mark.asyncio
    async def test_create_order_calculates_amount_correctly(
        self, create_order_use_case: CreateOrderUseCase, item_factory
    ):
        # Given
        items = [
            item_factory(price=Decimal("10.00")),
            item_factory(price=Decimal("15.50")),
            item_factory(price=Decimal("5.25")),
        ]
        user_id = str(uuid.uuid4())
        order_dto = OrderDTO(user_id=user_id, items=items)

        # When
        order = await create_order_use_case(order_dto)

        # Then
        assert order.amount == Decimal("30.75")

    @pytest.mark.asyncio
    async def test_create_order_creates_outbox_event(
        self,
        create_order_use_case: CreateOrderUseCase,
        outbox_repo: OutboxRepository,
        item_factory,
    ):
        # Given
        items = [item_factory()]
        user_id = str(uuid.uuid4())
        order_dto = OrderDTO(user_id=user_id, items=items)

        # When
        order = await create_order_use_case(order_dto)
        events = await outbox_repo.get_pending_events()

        # Then
        assert len(events) >= 1
        event = next(e for e in events if e.payload.get("id") == order.id)
        assert event.event_type == EventTypeEnum.ORDER_CREATED
        assert event.status == OutboxEventStatus.PENDING
        assert event.payload["id"] == order.id
        assert event.payload["user_id"] == user_id
        assert event.payload["amount"] == str(order.amount)

    @pytest.mark.asyncio
    async def test_create_order_with_empty_items(
        self, create_order_use_case: CreateOrderUseCase
    ):
        # Given
        user_id = str(uuid.uuid4())
        order_dto = OrderDTO(user_id=user_id, items=[])

        # When
        order = await create_order_use_case(order_dto)

        # Then
        assert order.amount == Decimal("0")
        assert order.items == []

    @pytest.mark.asyncio
    async def test_create_order_with_single_item(
        self, create_order_use_case: CreateOrderUseCase, item_factory
    ):
        # Given
        item = item_factory(price=Decimal("99.99"))
        user_id = str(uuid.uuid4())
        order_dto = OrderDTO(user_id=user_id, items=[item])

        # When
        order = await create_order_use_case(order_dto)

        # Then
        assert order.amount == Decimal("99.99")
        assert len(order.items) == 1
        assert order.items[0] == item

    @pytest.mark.asyncio
    async def test_create_multiple_orders_creates_multiple_events(
        self,
        create_order_use_case: CreateOrderUseCase,
        outbox_repo: OutboxRepository,
        item_factory,
    ):
        # Given
        user_id = str(uuid.uuid4())
        order_dto_1 = OrderDTO(user_id=user_id, items=[item_factory()])
        order_dto_2 = OrderDTO(user_id=user_id, items=[item_factory()])

        # When
        order_1 = await create_order_use_case(order_dto_1)
        order_2 = await create_order_use_case(order_dto_2)
        events = await outbox_repo.get_pending_events()

        # Then
        order_ids = {order_1.id, order_2.id}
        event_order_ids = {
            e.payload["id"]
            for e in events
            if e.event_type == EventTypeEnum.ORDER_CREATED
        }
        assert order_ids.issubset(event_order_ids)
