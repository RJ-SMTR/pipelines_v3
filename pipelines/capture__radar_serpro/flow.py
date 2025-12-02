# -*- coding: utf-8 -*-
import random
from time import sleep

import pandas as pd
from prefect import flow, task
from pipelines.capture__radar_serpro.tasks import (
    setup_serpro_cert,
    connect_to_serpro_db,
    test_serpro_connection
)



@flow(log_prints=True)
def capture__radar_serpro() -> list[str]:
    setup_serpro_cert()
    conn, cur = connect_to_serpro_db()
    test_serpro_connection(cur)
    sleep(360000)

