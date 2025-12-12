import uuid

from sqlalchemy import (
    JSON,
    UUID,
    Column,
    DateTime,
    MetaData,
    Table,
    Text,
    func,
)

metadata = MetaData()

shipments_tbl = Table(
    "shipments",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("order_id", Text, nullable=False, index=True),
    Column("tracking_number", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime, server_default=func.now()),
)

inbox_tbl = Table(
    "inbox",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("message_id", Text, nullable=False, unique=True, index=True),
    Column("event_type", Text, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime, server_default=func.now()),
)

outbox_tbl = Table(
    "outbox",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("event_type", Text, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("status", Text, nullable=False),
    Column("created_at", DateTime, server_default=func.now()),
)
