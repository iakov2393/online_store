from decimal import Decimal

from app.core.models import EventTypeEnum, PaymentStatusEnum
from app.infrastructure.repositories import (
    InboxRepository,
    OutboxRepository,
    PaymentRepository,
)
from app.infrastructure.unit_of_work import UnitOfWork


class ProcessOrderUseCase:
    def __init__(self, unit_of_work: UnitOfWork):
        self._unit_of_work = unit_of_work

    async def __call__(self, message_id: str, order_data: dict) -> None:
        async with self._unit_of_work() as uow:
            # Check idempotency
            if await uow.inbox.exists(message_id):
                return  # Already processed

            inbox_event = await uow.inbox.create(
                InboxRepository.CreateDTO(
                    message_id=message_id,
                    event_type="ORDER.CREATED",
                    payload=order_data,
                )
            )

            order_id = order_data.get("id")
            amount = Decimal(str(order_data.get("amount", "0")))

            order_id_hash = int(order_id.replace("-", ""), 16)

            if order_id_hash % 2 == 0:
                payment = await uow.payments.create(
                    PaymentRepository.CreateDTO(
                        order_id=order_id,
                        amount=amount,
                        status=PaymentStatusEnum.FAILED,
                        reason="INSUFFICIENT_FUNDS",
                    )
                )

                await uow.outbox.create(
                    OutboxRepository.CreateDTO(
                        event_type=EventTypeEnum.PAYMENT_FAILED,
                        payload={
                            "orderId": order_id,
                            "status": "CANCELLED",
                            "reason": "INSUFFICIENT_FUNDS",
                        },
                    )
                )
            else:
                payment = await uow.payments.create(
                    PaymentRepository.CreateDTO(
                        order_id=order_id,
                        amount=amount,
                        status=PaymentStatusEnum.PAID,
                    )
                )

                await uow.outbox.create(
                    OutboxRepository.CreateDTO(
                        event_type=EventTypeEnum.PAYMENT_SUCCESS,
                        payload={
                            "orderId": order_id,
                            "status": "PAID",
                            "amount": str(amount),
                        },
                    )
                )

            await uow.inbox.mark_as_processed(inbox_event.id)

            await uow.commit()
