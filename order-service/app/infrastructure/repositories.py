from decimal import Decimal

from pydantic import BaseModel
from sqlalchemy import Row, and_, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.models import (
    EventTypeEnum,
    Item,
    Order,
    OrderStatusEnum,
    OutboxEvent,
    OutboxEventStatus,
    InboxEvent,
    InboxEventStatus,
)
from app.infrastructure.db_schema import (
    order_statuses_tbl,
    orders_tbl,
    outbox_tbl,
    inbox_tbl,
)


class DoesNotExist(Exception):
    pass


class OrderRepository:
    class CreateDTO(BaseModel):
        user_id: str
        items: list[Item]
        amount: Decimal
        status: OrderStatusEnum

    def __init__(self, session: AsyncSession):
        self._session = session

    @staticmethod
    def _construct(row: Row | None) -> Order:
        if row is None:
            raise DoesNotExist

        return Order(
            id=str(row._mapping["id"]),
            user_id=row._mapping["user_id"],
            items=row._mapping["items"],
            amount=row._mapping["amount"],
            status=row._mapping["current_status"],
            status_history=row._mapping["status_history"],
        )

    async def create(self, order: CreateDTO) -> Order:
        stmt_order = (
            insert(orders_tbl)
            .values(
                {
                    "user_id": order.user_id,
                    "items": [item.model_dump(mode="json") for item in order.items],
                    "amount": order.amount,
                }
            )
            .returning(literal_column("*"))
        )
        result_order = await self._session.execute(stmt_order)
        order_row = result_order.fetchone()

        stmt_status = (
            insert(order_statuses_tbl)
            .values(
                {
                    "order_id": order_row.id,
                    "status": order.status,
                }
            )
            .returning(literal_column("*"))
        )
        await self._session.execute(stmt_status)

        order = await self.get_by_id(order_row.id)

        return order

    def _get_order_query(self):
        """Базовый query для получения заказов с последним статусом и историей"""
        # Подзапрос для последнего статуса
        latest_status_subq = select(
            order_statuses_tbl.c.order_id,
            order_statuses_tbl.c.status,
            order_statuses_tbl.c.created_at,
            func.row_number()
            .over(
                partition_by=order_statuses_tbl.c.order_id,
                order_by=order_statuses_tbl.c.created_at.desc(),
            )
            .label("rn"),
        ).subquery()

        # Подзапрос для истории статусов (JSON array)
        status_history_subq = (
            select(
                order_statuses_tbl.c.order_id,
                func.json_agg(
                    func.json_build_object(
                        "status",
                        order_statuses_tbl.c.status,
                        "created_at",
                        order_statuses_tbl.c.created_at,
                    ).op("ORDER BY")(order_statuses_tbl.c.created_at.desc())
                ).label("status_history"),
            )
            .group_by(order_statuses_tbl.c.order_id)
            .subquery()
        )

        return (
            select(
                orders_tbl,
                latest_status_subq.c.status.label("current_status"),
                status_history_subq.c.status_history,
            )
            .select_from(orders_tbl)
            .outerjoin(
                latest_status_subq,
                and_(
                    orders_tbl.c.id == latest_status_subq.c.order_id,
                    latest_status_subq.c.rn == 1,
                ),
            )
            .outerjoin(
                status_history_subq, orders_tbl.c.id == status_history_subq.c.order_id
            )
        )

    async def get_by_id(self, order_id: str) -> Order:
        stmt = self._get_order_query().where(orders_tbl.c.id == order_id)

        result = await self._session.execute(stmt)
        row = result.fetchone()

        if row is None:
            raise ValueError(f"Order with id {order_id} not found")

        return self._construct(row)


class OutboxRepository:
    class CreateDTO(BaseModel):
        event_type: EventTypeEnum
        payload: dict

    def __init__(self, session: AsyncSession):
        self._session = session

    @staticmethod
    def _construct(row: Row) -> OutboxEvent:
        if row is None:
            raise DoesNotExist

        return OutboxEvent(
            id=str(row._mapping["id"]),
            event_type=row._mapping["event_type"],
            payload=row._mapping["payload"],
            status=row._mapping["status"],
            created_at=str(row._mapping["created_at"]),
        )

    async def create(self, event: CreateDTO) -> OutboxEvent:
        stmt = (
            insert(outbox_tbl)
            .values(
                {
                    "event_type": event.event_type,
                    "payload": event.payload,
                    "status": OutboxEventStatus.PENDING,
                }
            )
            .returning(literal_column("*"))
        )
        result = await self._session.execute(stmt)
        row = result.fetchone()

        return self._construct(row)

    async def get_pending_events(self, limit: int = 100) -> list[OutboxEvent]:
        stmt = (
            select(outbox_tbl)
            .where(outbox_tbl.c.status == OutboxEventStatus.PENDING)
            .order_by(outbox_tbl.c.created_at)
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        rows = result.fetchall()

        return [self._construct(row) for row in rows]

    async def get_by_id(self, event_id: str) -> OutboxEvent:
        stmt = select(outbox_tbl).where(outbox_tbl.c.id == event_id)
        result = await self._session.execute(stmt)
        row = result.fetchone()

        return self._construct(row)

    async def mark_as_sent(self, event_id: str) -> None:
        stmt = (
            outbox_tbl.update()
            .where(outbox_tbl.c.id == event_id)
            .values(status=OutboxEventStatus.SENT)
        )
        await self._session.execute(stmt)


class InboxRepository:
    class CreateDTO(BaseModel):
        message_id: str
        event_type: str
        payload: dict

    def __init__(self, session: AsyncSession):
        self._session = session

    @staticmethod
    def _construct(row: Row | None) -> InboxEvent:
        if row is None:
            raise DoesNotExist

        return InboxEvent(
            id=str(row._mapping["id"]),
            message_id=row._mapping["message_id"],
            event_type=row._mapping["event_type"],
            payload=row._mapping["payload"],
            status=row._mapping["status"],
            created_at=row._mapping["created_at"],
        )

    async def exists(self, message_id: str) -> bool:
        """Check if message was already processed"""
        stmt = select(inbox_tbl.c.id).where(inbox_tbl.c.message_id == message_id)
        result = await self._session.execute(stmt)
        return result.fetchone() is not None

    async def create(self, event: CreateDTO) -> InboxEvent:
        stmt = (
            insert(inbox_tbl)
            .values(
                {
                    "message_id": event.message_id,
                    "event_type": event.event_type,
                    "payload": event.payload,
                    "status": InboxEventStatus.PENDING,
                }
            )
            .returning(literal_column("*"))
        )
        result = await self._session.execute(stmt)
        row = result.fetchone()

        return self._construct(row)

    async def mark_as_processed(self, event_id: str) -> None:
        stmt = (
            inbox_tbl.update()
            .where(inbox_tbl.c.id == event_id)
            .values(status=InboxEventStatus.PROCESSED)
        )
        await self._session.execute(stmt)
