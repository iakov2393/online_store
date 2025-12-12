import logging

from app.application.update_order_status import UpdateOrderStatusUseCase
from app.infrastructure.kafka_consumer import KafkaEventConsumer

logger = logging.getLogger(__name__)


class EventConsumerWorker:
    def __init__(
        self,
        update_status_use_case: UpdateOrderStatusUseCase,
        bootstrap_servers: str,
    ):
        self._update_status_use_case = update_status_use_case
        self._consumer = KafkaEventConsumer(
            bootstrap_servers=bootstrap_servers,
            topics=["payments", "shipments", "cancels"],
            group_id="order-service-consumer",
            process_message_callback=self._process_message,
        )

    async def _process_message(self, message_id: str, event_data: dict, topic: str):
        try:
            payload = event_data.get("payload", {})

            if topic in ["payments", "cancels"]:
                await self._update_status_use_case.handle_payment_event(
                    message_id=message_id, payment_data=payload
                )
            elif topic == "shipments":
                await self._update_status_use_case.handle_shipment_event(
                    message_id=message_id, shipment_data=payload
                )
            else:
                logger.warning(f"Unknown topic: {topic}")

        except Exception as e:
            logger.error(f"Error in message processing: {e}", exc_info=True)
            raise

    async def run(self):
        await self._consumer.start()
        try:
            await self._consumer.consume()
        finally:
            await self._consumer.stop()
