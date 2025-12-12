import asyncio
import logging
import os

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.application.process_outbox_events import ProcessOutboxEventsUseCase
from app.application.process_payment import ProcessPaymentUseCase
from app.infrastructure.kafka_consumer import KafkaPaymentConsumer
from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.unit_of_work import UnitOfWork
from app.presentation.outbox_worker import OutboxWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ShippingWorker:
    def __init__(
        self,
        process_payment_use_case: ProcessPaymentUseCase,
        bootstrap_servers: str,
        payments_topic: str,
    ):
        self._process_payment_use_case = process_payment_use_case
        self._consumer = KafkaPaymentConsumer(
            bootstrap_servers=bootstrap_servers,
            topic=payments_topic,
            group_id="shipping-service-consumer",
            process_message_callback=self._process_message,
        )

    async def _process_message(self, message_id: str, payment_data: dict):
        try:
            await self._process_payment_use_case(
                message_id=message_id, payment_data=payment_data
            )
        except Exception as e:
            logger.error(f"Error processing payment: {e}", exc_info=True)
            raise

    async def run(self):
        await self._consumer.start()
        try:
            await self._consumer.consume()
        finally:
            await self._consumer.stop()


async def main():
    # Database setup
    db_dsn = f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
    engine = create_async_engine(db_dsn, pool_size=15, pool_recycle=1800)
    session_factory = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    payments_topic = os.getenv("KAFKA_PAYMENTS_TOPIC", "payments")
    shipments_topic = os.getenv("KAFKA_SHIPMENTS_TOPIC", "shipments")

    uow = UnitOfWork(session_factory)
    process_payment_use_case = ProcessPaymentUseCase(unit_of_work=uow)

    kafka_producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=shipments_topic,
    )

    process_outbox_use_case = ProcessOutboxEventsUseCase(
        unit_of_work=uow,
        kafka_producer=kafka_producer,
    )

    shipping_worker = ShippingWorker(
        process_payment_use_case=process_payment_use_case,
        bootstrap_servers=bootstrap_servers,
        payments_topic=payments_topic,
    )

    outbox_worker = OutboxWorker(use_case=process_outbox_use_case)

    logger.info("Starting Shipping Service...")
    consumer_task = asyncio.create_task(shipping_worker.run())
    outbox_task = asyncio.create_task(outbox_worker.run())

    await asyncio.gather(consumer_task, outbox_task)


if __name__ == "__main__":
    asyncio.run(main())
