import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)


class KafkaPaymentConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        process_message_callback,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._process_message = process_message_callback
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        await self._consumer.start()

    async def stop(self):
        if self._consumer:
            await self._consumer.stop()

    async def consume(self):
        if not self._consumer:
            raise RuntimeError("Consumer not started")

        logger.info(f"Started consuming from topic: {self._topic}")

        try:
            async for message in self._consumer:
                try:
                    message_id = message.key.decode("utf-8") if message.key else None
                    event_data = message.value

                    payload = event_data.get("payload", {})

                    logger.info(
                        f"Processing message: {message_id}, order_id: {payload.get('orderId')}"
                    )

                    await self._process_message(
                        message_id=message_id, payment_data=payload
                    )

                except Exception as e:
                    logger.error(
                        f"Error processing message {message_id}: {e}", exc_info=True
                    )
                    continue

        except asyncio.CancelledError:
            logger.info("Consumer cancelled")
            raise
        except Exception as e:
            logger.error(f"Fatal error in consumer: {e}", exc_info=True)
            raise
