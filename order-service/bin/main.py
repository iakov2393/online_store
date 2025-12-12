import asyncio

import uvicorn
from fastapi import FastAPI

from app.application.container import ApplicationContainer
from app.application.update_order_status import UpdateOrderStatusUseCase
from app.presentation import api
from app.presentation.api import router
from app.presentation.container import PresentationContainer
from app.presentation.outbox_worker import OutboxWorker
from app.presentation.event_consumer_worker import EventConsumerWorker
import os


def build_api(container: ApplicationContainer):
    app = FastAPI()
    app.include_router(router)
    container.wire(modules=[api])
    return app


async def main():
    presentation_container = PresentationContainer()
    presentation_container.config.from_yaml("app/config.yaml", required=True)

    app = build_api(presentation_container.application)

    outbox_worker: OutboxWorker = presentation_container.outbox_worker()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    update_status_use_case = UpdateOrderStatusUseCase(
        unit_of_work=presentation_container.application.infrastructure_container.unit_of_work()
    )

    event_consumer = EventConsumerWorker(
        update_status_use_case=update_status_use_case,
        bootstrap_servers=bootstrap_servers,
    )

    api_task = asyncio.create_task(
        uvicorn.Server(
            uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
        ).serve()
    )

    outbox_task = asyncio.create_task(outbox_worker.run())

    consumer_task = asyncio.create_task(event_consumer.run())

    await asyncio.gather(api_task, outbox_task, consumer_task)


if __name__ == "__main__":
    asyncio.run(main())
