from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.unit_of_work import UnitOfWork


class ProcessOutboxEventsUseCase:
    def __init__(
        self,
        unit_of_work: UnitOfWork,
        kafka_producer: KafkaProducer,
        batch_size: int = 100,
    ):
        self._unit_of_work = unit_of_work
        self._kafka_producer = kafka_producer
        self._batch_size = batch_size

    async def __call__(self) -> None:
        """
        Process outbox events: fetch new events, send to Kafka, mark as sent.
        Returns the number of events processed.
        """
        async with self._unit_of_work() as uow:
            # Fetch new events from outbox
            events = await uow.outbox.get_pending_events(limit=self._batch_size)

            if not events:
                return

        async with self._kafka_producer as kp:
            for event in events:
                async with self._unit_of_work() as uow:
                    try:
                        await kp.send_message(
                            message={
                                "event_type": event.event_type,
                                "payload": event.payload,
                                "created_at": event.created_at.isoformat(),
                            },
                            key=event.id,
                        )
                    except Exception as e:
                        # Log error but continue processing other events
                        print(f"Failed to send event {event.id}: {e}")
                        # Could implement retry logic or error handling here
                        continue

                    # Mark as sent
                    await uow.outbox.mark_as_sent(event.id)

                    # Commit all changes
                    await uow.commit()
