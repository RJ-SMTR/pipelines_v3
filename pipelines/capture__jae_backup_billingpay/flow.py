# -*- coding: utf-8 -*-
"""
Flow para backup incremental de dados BillingPay da Jaé
Common: 2026-03-11a
"""

from datetime import datetime
from typing import Optional

from prefect import flow

from pipelines.capture__jae_backup_billingpay.tasks import (
    create_non_filtered_discord_message,
    get_jae_db_config,
    get_non_filtered_tables,
    get_raw_backup_billingpay,
    get_table_info,
    set_redis_backup_billingpay,
    upload_backup_billingpay,
)
from pipelines.capture__jae_backup_billingpay.utils import get_backup_billing_pay_flow_run_name
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
    task_send_discord_message,
)


@flow(log_prints=True, flow_run_name=get_backup_billing_pay_flow_run_name)
def capture__jae_backup_billingpay(
    database_name: str,
    env: Optional[str] = None,
    end_datetime: Optional[datetime] = None,
):
    """
    Flow para backup incremental de dados BillingPay da Jaé

    Args:
        database_name (str): Nome do banco de dados a fazer backup
        end_datetime (Optional[datetime]): Data/hora final (default: timestamp agora)
    """
    initialize_sentry(env=env)
    setup_environment(env=env)

    env = get_run_env(env=env)
    database_config = get_jae_db_config(database_name=database_name)
    timestamp = get_scheduled_timestamp(timestamp=end_datetime)

    table_info = get_table_info(
        env=env,
        database_name=database_name,
        database_config=database_config,
        timestamp=timestamp,
    )

    send_message, table_count = get_non_filtered_tables(
        database_name=database_name,
        database_config=database_config,
        table_info=table_info,
    )

    if send_message:
        message = create_non_filtered_discord_message(
            database_name=database_name,
            table_count=table_count,
        )
        task_send_discord_message(message=message, webhook=jae_constants.ALERT_WEBHOOK)

    table_info = get_raw_backup_billingpay(
        table_info=table_info,
        database_config=database_config,
        timestamp=timestamp,
    )

    table_info = upload_backup_billingpay.map(
        env=env,
        table_info=table_info,
        database_name=database_name,
    )

    set_redis_backup_billingpay.map(
        env=env,
        table_info=table_info,
        database_name=database_name,
        timestamp=timestamp,
    )
