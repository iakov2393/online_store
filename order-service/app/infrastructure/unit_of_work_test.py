import uuid
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.models import EventTypeEnum, OrderStatusEnum
from app.infrastructure.repositories import OrderRepository, OutboxRepository
from app.infrastructure.unit_of_work import UnitOfWork


@pytest.fixture
async def uow(session_factory: async_sessionmaker[AsyncSession]) -> UnitOfWork:
    return UnitOfWork(session_factory)


class TestUnitOfWork:
    @pytest.mark.asyncio
    async def test_provides_order_repository(self, uow: UnitOfWork):
        # Given/When
        async with uow() as unit:
            # Then
            assert isinstance(unit.orders, OrderRepository)

    @pytest.mark.asyncio
    async def test_provides_outbox_repository(self, uow: UnitOfWork):
        # Given/When
        async with uow() as unit:
            # Then
            assert isinstance(unit.outbox, OutboxRepository)

    @pytest.mark.asyncio
    async def test_commit_persists_changes(self, uow: UnitOfWork, item_factory):
        # Given
        items = [item_factory()]
        user_id = str(uuid.uuid4())
        order_id = None

        # When
        async with uow() as unit:
            order = await unit.orders.create(
                OrderRepository.CreateDTO(
                    user_id=user_id,
                    items=items,
                    amount=Decimal("10.50"),
                    status=OrderStatusEnum.NEW,
                )
            )
            order_id = order.id
            await unit.commit()

        # Then - verify order persisted in new session
        async with uow() as unit:
            persisted_order = await unit.orders.get_by_id(order_id)
            assert persisted_order.id == order_id
            assert persisted_order.user_id == user_id

    @pytest.mark.asyncio
    async def test_rollback_on_exception(self, uow: UnitOfWork, item_factory):
        # Given
        items = [item_factory()]
        user_id = str(uuid.uuid4())

        # When
        with pytest.raises(Exception, match="Test error"):
            async with uow() as unit:
                order = await unit.orders.create(
                    OrderRepository.CreateDTO(
                        user_id=user_id,
                        items=items,
                        amount=Decimal("10.50"),
                        status=OrderStatusEnum.NEW,
                    )
                )
                order_id = order.id
                raise Exception("Test error")

        # Then - verify order was rolled back
        async with uow() as unit:
            with pytest.raises(ValueError, match="not found"):
                await unit.orders.get_by_id(order_id)

    @pytest.mark.asyncio
    async def test_transaction_isolation(self, uow: UnitOfWork, item_factory):
        # Given
        items = [item_factory()]
        user_id = str(uuid.uuid4())

        # When - create order but don't commit
        async with uow() as unit:
            order = await unit.orders.create(
                OrderRepository.CreateDTO(
                    user_id=user_id,
                    items=items,
                    amount=Decimal("10.50"),
                    status=OrderStatusEnum.NEW,
                )
            )
            order_id = order.id
            # Don't call commit - context exit will rollback

        # Then - order should not exist in new session
        async with uow() as unit:
            with pytest.raises(ValueError, match="not found"):
                await unit.orders.get_by_id(order_id)

    @pytest.mark.asyncio
    async def test_multiple_operations_in_single_transaction(
        self, uow: UnitOfWork, item_factory
    ):
        # Given
        items = [item_factory()]
        user_id = str(uuid.uuid4())

        # When
        async with uow() as unit:
            order = await unit.orders.create(
                OrderRepository.CreateDTO(
                    user_id=user_id,
                    items=items,
                    amount=Decimal("10.50"),
                    status=OrderStatusEnum.NEW,
                )
            )

            await unit.outbox.create(
                OutboxRepository.CreateDTO(
                    event_type=EventTypeEnum.ORDER_CREATED,
                    payload={"order_id": order.id},
                )
            )

            await unit.commit()

        # Then - both operations should be persisted
        async with uow() as unit:
            persisted_order = await unit.orders.get_by_id(order.id)
            assert persisted_order.id == order.id

            events = await unit.outbox.get_pending_events()
            assert len(events) >= 1
            assert any(e.payload.get("order_id") == order.id for e in events)

    @pytest.mark.asyncio
    async def test_rollback_affects_all_operations(self, uow: UnitOfWork, item_factory):
        # Given
        items = [item_factory()]
        user_id = str(uuid.uuid4())
        order_id = None

        # When
        with pytest.raises(Exception, match="Test error"):
            async with uow() as unit:
                order = await unit.orders.create(
                    OrderRepository.CreateDTO(
                        user_id=user_id,
                        items=items,
                        amount=Decimal("10.50"),
                        status=OrderStatusEnum.NEW,
                    )
                )
                order_id = order.id

                await unit.outbox.create(
                    OutboxRepository.CreateDTO(
                        event_type=EventTypeEnum.ORDER_CREATED,
                        payload={"order_id": order.id},
                    )
                )

                raise Exception("Test error")

        # Then - neither order nor event should exist
        async with uow() as unit:
            with pytest.raises(ValueError, match="not found"):
                await unit.orders.get_by_id(order_id)

            events = await unit.outbox.get_pending_events()
            assert not any(e.payload.get("order_id") == order_id for e in events)

    @pytest.mark.asyncio
    async def test_same_repositories_within_transaction(self, uow: UnitOfWork):
        # Given/When
        async with uow() as unit:
            repo1 = unit.orders
            repo2 = unit.orders

            # Then
            assert repo1 is repo2  # Same instance within transaction
