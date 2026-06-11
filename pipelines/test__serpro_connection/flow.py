# -*- coding: utf-8 -*-
import socket
from pathlib import Path
from time import sleep

from pipelines.common.tasks import setup_environment
from pipelines.common.utils.prefect import flow


def _get_kubectl_exec_cmd() -> str:
    pod = socket.gethostname()
    namespace_file = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    namespace = (
        namespace_file.read_text().strip() if namespace_file.exists() else "prefect-worker-v3"
    )
    return f"kubectl exec -it -n {namespace} {pod} -- bash"


@flow(log_prints=True)
def test__serpro_connection(sleep_seconds: int = 14400):  # noqa: PT028
    setup_environment(env="dev")
    print(f"Acesse o pod com:\n\n    {_get_kubectl_exec_cmd()}\n")
    print("""Dentro do pod:
    uv run python3 utils.py

Exemplo de query:
    q('''
    SELECT
        auinf_dt_infracao,
        auinf_num_auto,
        pag_data_pagamento,
        pag_status_pagamento,
        data_atualizacao_dl
    FROM dbpro_radar_view.tb_infracao_view_smtr
    WHERE auinf_dt_infracao >= '2023-06-09'
      AND auinf_dt_infracao < '2023-06-10'
      AND auinf_num_auto IN (
        'RA20058024', 'RA20052044', 'RA10074201', 'RA20057119', 'RA10074639',
        'RA10076541', 'RA20058319', 'RA30011031', 'RA30011071', 'RA10075840',
        'RE40001263'
      )
      OR data_atualizacao_dl = '2026-01-01'
    ''')
""")
    print(f"Dormindo por {sleep_seconds}s ({sleep_seconds // 3600}h)...")
    sleep(sleep_seconds)
