# -*- coding: utf-8 -*-
"""
REPL interativo para queries no SERPRO.
Uso dentro do pod: uv run python3 utils.py
"""

import code
import os

from impala.dbapi import connect

from pipelines.capture__serpro_autuacao.constants import SERPRO_CAPTURE_PARAMS
from pipelines.capture__serpro_autuacao.tasks import _write_serpro_certificate

PAGE_SIZE = SERPRO_CAPTURE_PARAMS["page_size"]


def q(sql: str, page_size: int = PAGE_SIZE):
    """
    Executa uma query no SERPRO e imprime até page_size linhas.

    Args:
        sql: Query SQL a ser executada
        page_size: Quantidade máxima de linhas a retornar

    Returns:
        list: Linhas retornadas pela query
    """
    cur.execute(sql)
    rows = cur.fetchmany(page_size)
    if cur.description:
        cols = [d[0] for d in cur.description]
        print(" | ".join(cols))
        print("-" * (sum(len(c) for c in cols) + 3 * len(cols)))
    for row in rows:
        print(" | ".join(str(v) for v in row))
    print(f"\n({len(rows)} rows, page_size={page_size})")
    return rows


_crt_path = _write_serpro_certificate(os.environ["radar_serpro_v2_crt"])

with connect(
    host=os.environ["radar_serpro_v2_host"],
    port=int(os.environ["radar_serpro_v2_port"]),
    user=os.environ["radar_serpro_v2_user"],
    password=os.environ["radar_serpro_v2_password"],
    auth_mechanism="LDAP",
    use_ssl=True,
    ca_cert=_crt_path,
    database=os.environ["radar_serpro_v2_database"],
) as conn:
    with conn.cursor() as cur:
        print(f"""
        Conectado ao SERPRO. (page_size={PAGE_SIZE})

          q('SELECT ...')                    — executa e retorna até {PAGE_SIZE} rows
          q('SELECT ...', page_size=10)      — page_size customizado
          cur.execute('SELECT ...') + cur.fetchall()  — acesso direto ao cursor
          conn.close()                       — encerra a conexão
        """)

        code.interact(local={"conn": conn, "cur": cur, "q": q})
