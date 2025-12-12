from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel


class PaymentStatusEnum(StrEnum):
    PENDING = "PENDING"
    PAID = "PAID"
    FAILED = "FAILED"


class Payment(BaseModel):
    id: str
    order_id: str
    amount: Decimal
    status: PaymentStatusEnum
    reason: str | None = None
    created_at: datetime


class EventTypeEnum(StrEnum):
    PAYMENT_SUCCESS = "PAYMENT.SUCCESS"
    PAYMENT_FAILED = "PAYMENT.FAILED"


class InboxEventStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSED = "PROCESSED"


class InboxEvent(BaseModel):
    id: str
    message_id: str
    event_type: str
    payload: dict
    status: InboxEventStatus
    created_at: datetime


class OutboxEventStatus(StrEnum):
    PENDING = "PENDING"
    SENT = "SENT"


class OutboxEvent(BaseModel):
    id: str
    event_type: EventTypeEnum
    payload: dict
    status: OutboxEventStatus
    created_at: datetime
