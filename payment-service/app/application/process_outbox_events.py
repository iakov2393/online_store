from app.core.models import EventTypeEnum
from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.unit_of_work import UnitOfWork


class ProcessOutboxEventsUseCase:
    def __init__(
        self,
        unit_of_work: UnitOfWork,
        kafka_producer: KafkaProducer,
        payments_topic: str,
        cancels_topic: str,
        batch_size: int = 100,
    ):
        self._unit_of_work = unit_of_work
        self._kafka_producer = kafka_producer
        self._payments_topic = payments_topic
        self._cancels_topic = cancels_topic
        self._batch_size = batch_size

    async def __call__(self) -> None:
        async with self._unit_of_work() as uow:
            events = await uow.outbox.get_pending_events(limit=self._batch_size)

            if not events:
                return

        async with self._kafka_producer as kp:
            for event in events:
                async with self._unit_of_work() as uow:
                    try:
                        if event.event_type == EventTypeEnum.PAYMENT_SUCCESS:
                            topic = self._payments_topic
                        elif event.event_type == EventTypeEnum.PAYMENT_FAILED:
                            topic = self._cancels_topic
                        else:
                            topic = self._payments_topic

                        await kp.send_message(
                            message={
                                "event_type": event.event_type,
                                "payload": event.payload,
                                "created_at": event.created_at.isoformat(),
                            },
                            key=event.id,
                            topic=topic,
                        )
                    except Exception as e:
                        print(f"Failed to send event {event.id}: {e}")
                        continue

                    await uow.outbox.mark_as_sent(event.id)
                    await uow.commit()
