import asyncio
import logging

from app.application.process_order import ProcessOrderUseCase
from app.infrastructure.kafka_consumer import KafkaOrderConsumer
from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.unit_of_work import UnitOfWork
from app.presentation.outbox_worker import OutboxWorker
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PaymentWorker:
    def __init__(
        self,
        process_order_use_case: ProcessOrderUseCase,
        bootstrap_servers: str,
        orders_topic: str,
    ):
        self._process_order_use_case = process_order_use_case
        self._consumer = KafkaOrderConsumer(
            bootstrap_servers=bootstrap_servers,
            topic=orders_topic,
            group_id="payment-service-consumer",
            process_message_callback=self._process_message,
        )

    async def _process_message(self, message_id: str, order_data: dict):
        try:
            await self._process_order_use_case(
                message_id=message_id, order_data=order_data
            )
        except Exception as e:
            logger.error(f"Error processing order: {e}", exc_info=True)
            raise

    async def run(self):
        await self._consumer.start()
        try:
            await self._consumer.consume()
        finally:
            await self._consumer.stop()


async def main():
    db_dsn = f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
    engine = create_async_engine(db_dsn, pool_size=15, pool_recycle=1800)
    session_factory = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    orders_topic = os.getenv("KAFKA_ORDERS_TOPIC", "orders")
    payments_topic = os.getenv("KAFKA_PAYMENTS_TOPIC", "payments")
    cancels_topic = os.getenv("KAFKA_CANCELS_TOPIC", "cancels")

    uow = UnitOfWork(session_factory)
    process_order_use_case = ProcessOrderUseCase(unit_of_work=uow)

    kafka_producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=payments_topic,
    )

    from app.application.process_outbox_events import ProcessOutboxEventsUseCase

    process_outbox_use_case = ProcessOutboxEventsUseCase(
        unit_of_work=uow,
        kafka_producer=kafka_producer,
        payments_topic=payments_topic,
        cancels_topic=cancels_topic,
    )

    payment_worker = PaymentWorker(
        process_order_use_case=process_order_use_case,
        bootstrap_servers=bootstrap_servers,
        orders_topic=orders_topic,
    )

    outbox_worker = OutboxWorker(use_case=process_outbox_use_case)

    logger.info("Starting Payment Service...")
    consumer_task = asyncio.create_task(payment_worker.run())
    outbox_task = asyncio.create_task(outbox_worker.run())

    await asyncio.gather(consumer_task, outbox_task)


if __name__ == "__main__":
    asyncio.run(main())
