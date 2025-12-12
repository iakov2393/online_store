import uuid

from sqlalchemy import (
    DECIMAL,
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

payments_tbl = Table(
    "payments",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("order_id", Text, nullable=False, index=True),
    Column("amount", DECIMAL(10, 2), nullable=False),
    Column("status", Text, nullable=False),
    Column("reason", Text, nullable=True),
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
