import json
from typing import Any

from aiokafka import AIOKafkaProducer


class KafkaProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer: AIOKafkaProducer | None = None

    async def start(self):
        """Initialize and start the Kafka producer"""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self._producer.start()

    async def stop(self):
        """Stop the Kafka producer"""
        if self._producer:
            await self._producer.stop()

    async def send_message(
        self,
        message: dict[str, Any],
        key: str | None = None,
        topic: str | None = None,
    ) -> None:
        """Send a message to Kafka"""
        if not self._producer:
            raise RuntimeError("Producer is not started. Call start() first.")

        target_topic = topic or self._topic
        await self._producer.send_and_wait(
            topic=target_topic,
            value=message,
            key=key,
        )

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
