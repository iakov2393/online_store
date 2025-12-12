import uuid

from app.core.models import EventTypeEnum, ShipmentStatusEnum
from app.infrastructure.repositories import (
    InboxRepository,
    OutboxRepository,
    ShipmentRepository,
)
from app.infrastructure.unit_of_work import UnitOfWork


class ProcessPaymentUseCase:
    def __init__(self, unit_of_work: UnitOfWork):
        self._unit_of_work = unit_of_work

    async def __call__(self, message_id: str, payment_data: dict) -> None:
        async with self._unit_of_work() as uow:
            if await uow.inbox.exists(message_id):
                return  # Already processed

            # Save to inbox
            inbox_event = await uow.inbox.create(
                InboxRepository.CreateDTO(
                    message_id=message_id,
                    event_type="PAYMENT.SUCCESS",
                    payload=payment_data,
                )
            )

            order_id = payment_data.get("orderId")

            tracking_number = f"SHIP-{uuid.uuid4().hex[:8].upper()}"

            shipment = await uow.shipments.create(
                ShipmentRepository.CreateDTO(
                    order_id=order_id,
                    tracking_number=tracking_number,
                    status=ShipmentStatusEnum.SHIPPED,
                )
            )

            await uow.outbox.create(
                OutboxRepository.CreateDTO(
                    event_type=EventTypeEnum.ORDER_SHIPPED,
                    payload={
                        "orderId": order_id,
                        "status": "SHIPPED",
                        "trackingNumber": tracking_number,
                    },
                )
            )

            await uow.inbox.mark_as_processed(inbox_event.id)

            await uow.commit()
