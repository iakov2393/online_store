import uuid

from sqlalchemy import (
    DECIMAL,
    JSON,
    UUID,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    Text,
    func,
)

metadata = MetaData()

orders_tbl = Table(
    "orders",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("user_id", Text, nullable=False),
    Column("items", JSON, nullable=False),
    Column("amount", DECIMAL(10, 2), nullable=False),
    Column("created_at", DateTime, server_default=func.now()),
)

order_statuses_tbl = Table(
    "order_statuses",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("order_id", UUID(as_uuid=True), ForeignKey("orders.id")),
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
