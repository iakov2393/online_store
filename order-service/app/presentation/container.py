from dependency_injector import containers, providers

from app.application.container import ApplicationContainer
from app.presentation.outbox_worker import OutboxWorker


class PresentationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    application = providers.Container[ApplicationContainer](
        ApplicationContainer, config=config
    )

    outbox_worker = providers.Singleton[OutboxWorker](
        OutboxWorker, use_case=application.process_outbox_events_use_case
    )
