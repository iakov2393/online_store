from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel


class ShipmentStatusEnum(StrEnum):
    PENDING = "PENDING"
    SHIPPED = "SHIPPED"


class Shipment(BaseModel):
    id: str
    order_id: str
    tracking_number: str
    status: ShipmentStatusEnum
    created_at: datetime


class EventTypeEnum(StrEnum):
    ORDER_SHIPPED = "ORDER.SHIPPED"


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
