from decimal import Decimal

from pydantic import BaseModel
from sqlalchemy import Row, literal_column, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.models import (
    EventTypeEnum,
    InboxEvent,
    InboxEventStatus,
    OutboxEvent,
    OutboxEventStatus,
    Payment,
    PaymentStatusEnum,
)
from app.infrastructure.db_schema import inbox_tbl, outbox_tbl, payments_tbl


class DoesNotExist(Exception):
    pass


class PaymentRepository:
    class CreateDTO(BaseModel):
        order_id: str
        amount: Decimal
        status: PaymentStatusEnum
        reason: str | None = None

    def __init__(self, session: AsyncSession):
        self._session = session

    @staticmethod
    def _construct(row: Row | None) -> Payment:
        if row is None:
            raise DoesNotExist

        return Payment(
            id=str(row._mapping["id"]),
            order_id=row._mapping["order_id"],
            amount=row._mapping["amount"],
            status=row._mapping["status"],
            reason=row._mapping["reason"],
            created_at=row._mapping["created_at"],
        )

    async def create(self, payment: CreateDTO) -> Payment:
        stmt = (
            insert(payments_tbl)
            .values(
                {
                    "order_id": payment.order_id,
                    "amount": payment.amount,
                    "status": payment.status,
                    "reason": payment.reason,
                }
            )
            .returning(literal_column("*"))
        )
        result = await self._session.execute(stmt)
        row = result.fetchone()

        return self._construct(row)

    async def get_by_order_id(self, order_id: str) -> Payment | None:
        stmt = select(payments_tbl).where(payments_tbl.c.order_id == order_id)
        result = await self._session.execute(stmt)
        row = result.fetchone()

        if row is None:
            return None

        return self._construct(row)


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
            created_at=row._mapping["created_at"],
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

    async def mark_as_sent(self, event_id: str) -> None:
        stmt = (
            outbox_tbl.update()
            .where(outbox_tbl.c.id == event_id)
            .values(status=OutboxEventStatus.SENT)
        )
        await self._session.execute(stmt)
