from dispatcher.utils import get_redis_client
from rq.worker import Worker


class NOJWorker(Worker):

    def __init__(
        self,
        queues,
        name=None,
        connection=None,
        default_result_ttl=...,
        exc_handler=None,
        exception_handlers=None,
        default_worker_ttl=...,
        job_class=None,
        queue_class=None,
        log_job_description=True,
        job_monitoring_interval=...,
        disable_default_exception_handler=False,
        prepare_for_work=True,
        serializer=None,
    ):
        # Forward init parameter
        super().__init__(
            queues=queues,
            name=name,
            connection=get_redis_client(),
            default_result_ttl=default_result_ttl,
            exc_handler=exc_handler,
            exception_handlers=exception_handlers,
            default_worker_ttl=default_worker_ttl,
            job_class=job_class,
            queue_class=queue_class,
            log_job_description=log_job_description,
            job_monitoring_interval=job_monitoring_interval,
            disable_default_exception_handler=disable_default_exception_handler,
            prepare_for_work=prepare_for_work,
            serializer=serializer,
        )
