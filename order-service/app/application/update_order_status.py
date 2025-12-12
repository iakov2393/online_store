from app.core.models import OrderStatusEnum
from app.infrastructure.repositories import InboxRepository
from app.infrastructure.unit_of_work import UnitOfWork
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import literal_column
from app.infrastructure.db_schema import order_statuses_tbl


class UpdateOrderStatusUseCase:
    def __init__(self, unit_of_work: UnitOfWork):
        self._unit_of_work = unit_of_work

    async def handle_payment_event(self, message_id: str, payment_data: dict) -> None:
        async with self._unit_of_work() as uow:
            if await uow.inbox.exists(message_id):
                return

            inbox_event = await uow.inbox.create(
                InboxRepository.CreateDTO(
                    message_id=message_id,
                    event_type="PAYMENT",
                    payload=payment_data,
                )
            )

            order_id = payment_data.get("orderId")
            status_str = payment_data.get("status")

            if status_str == "PAID":
                new_status = OrderStatusEnum.PAID
            elif status_str == "CANCELLED":
                new_status = OrderStatusEnum.CANCELLED
            else:
                await uow.inbox.mark_as_processed(inbox_event.id)
                await uow.commit()
                return

            stmt = insert(order_statuses_tbl).values(
                {
                    "order_id": order_id,
                    "status": new_status,
                }
            )
            await uow._session.execute(stmt)

            await uow.inbox.mark_as_processed(inbox_event.id)
            await uow.commit()

    async def handle_shipment_event(self, message_id: str, shipment_data: dict) -> None:
        async with self._unit_of_work() as uow:
            # Check idempotency
            if await uow.inbox.exists(message_id):
                return

            inbox_event = await uow.inbox.create(
                InboxRepository.CreateDTO(
                    message_id=message_id,
                    event_type="SHIPMENT",
                    payload=shipment_data,
                )
            )

            order_id = shipment_data.get("orderId")

            stmt = insert(order_statuses_tbl).values(
                {
                    "order_id": order_id,
                    "status": OrderStatusEnum.SHIPPED,
                }
            )
            await uow._session.execute(stmt)

            await uow.inbox.mark_as_processed(inbox_event.id)
            await uow.commit()
