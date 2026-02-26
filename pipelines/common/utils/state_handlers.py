import sentry_sdk
from prefect.logging.loggers import flow_run_logger

from pipelines.common.utils.secret import get_secret

def handler_post_sentry(flow, flow_run, state):

    def handler(flow, flow_run, state):  # noqa: ARG001
        sentry_dsn = get_secret("sentry", "dsn")['dsn']
        environment = 'staging'
        # flow_run_logger.info('Inicializando sentry_sdk com DSN:', sentry_dsn)
        sentry_sdk.init(
            dsn=sentry_dsn,
            traces_sample_rate=0,
            environment=environment,
        )

    return handler