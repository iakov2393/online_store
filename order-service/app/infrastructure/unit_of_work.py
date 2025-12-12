from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.repositories import (
    OrderRepository,
    OutboxRepository,
    InboxRepository,
)


class UnitOfWork:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @asynccontextmanager
    async def __call__(self):
        async with self._session_factory() as session:
            try:
                yield _UnitOfWorkImplementation(session)
                # Rollback if commit wasn't explicitly called
                await session.rollback()
            except Exception:
                await session.rollback()
                raise


class _UnitOfWorkImplementation:
    def __init__(self, session: AsyncSession):
        self._session = session
        self._order_repo = OrderRepository(session)
        self._outbox_repo = OutboxRepository(session)
        self._inbox_repo = InboxRepository(session)

    @property
    def orders(self) -> OrderRepository:
        return self._order_repo

    @property
    def outbox(self) -> OutboxRepository:
        return self._outbox_repo

    @property
    def inbox(self) -> InboxRepository:  # Add this
        return self._inbox_repo

    async def commit(self):
        await self._session.commit()
