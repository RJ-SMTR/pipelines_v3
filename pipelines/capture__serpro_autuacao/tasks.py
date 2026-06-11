# -*- coding: utf-8 -*-
"""
Tasks para captura de dados do SERPRO
"""

import os
import socket as _socket
import ssl as _ssl
from datetime import datetime
from functools import partial
from pathlib import Path

import pandas as pd
from impala.dbapi import connect
from prefect import task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NO_CACHE

from pipelines.capture__serpro_autuacao.constants import SERPRO_CAPTURE_PARAMS
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.fs import save_local_file


def _write_serpro_certificate(content: str) -> str:
    """
    Grava o conteúdo do certificado SSL do SERPRO em arquivo local.

    Args:
        content: Conteúdo PEM do certificado ou cadeia de certificados

    Returns:
        str: Caminho local do certificado gravado
    """
    crt_local_path = os.environ["radar_serpro_v2_crt_local_path"]
    Path(crt_local_path).parent.mkdir(parents=True, exist_ok=True)
    content = content.replace("\\n", "\n").strip() + "\n"
    Path(crt_local_path).write_text(content)
    print(f"Certificado baixado em {crt_local_path}")
    return crt_local_path


def _certificate_to_pem(certificate) -> str:
    """
    Converte um certificado retornado pelo socket SSL para PEM.

    Args:
        certificate: Certificado em DER ou objeto de certificado do Python

    Returns:
        str: Certificado no formato PEM
    """
    if isinstance(certificate, bytes):
        return _ssl.DER_cert_to_PEM_cert(certificate)

    pem = certificate.public_bytes()
    return pem.decode() if isinstance(pem, bytes) else pem


def _fetch_serpro_certificate_chain(host: str, port: int) -> str:
    """
    Recupera a cadeia de certificados apresentada pelo servidor SERPRO.

    Args:
        host: Host do servidor SERPRO
        port: Porta do servidor SERPRO

    Returns:
        str: Cadeia de certificados no formato PEM
    """
    context = _ssl._create_unverified_context()
    with _socket.create_connection((host, port), timeout=10) as sock:
        with context.wrap_socket(sock, server_hostname=host) as ssl_sock:
            certificates = ssl_sock.get_unverified_chain()

    return "\n".join(_certificate_to_pem(certificate).strip() for certificate in certificates)


def _create_serpro_certificate_artifact(content: str) -> None:
    """
    Cria um artefato no Prefect com a cadeia de certificados recuperada.

    Args:
        content: Conteúdo PEM da cadeia de certificados recuperada
    """
    try:
        create_markdown_artifact(
            key="serpro-certificate-chain",
            description="Nova cadeia de certificados do SERPRO para atualizar no Infisical",
            markdown=f"""# Nova cadeia de certificados do SERPRO

Atualize o secret `radar_serpro_v2_crt` no Infisical com o conteúdo abaixo.

```pem
{content.strip()}
```
""",
        )
    except Exception as e:
        print(f"Falha ao criar artefato de certificado SERPRO: {type(e).__name__}: {e}")


def _recover_serpro_certificate(host: str, port: int) -> str:
    """
    Recupera, valida e grava a cadeia atual de certificados do SERPRO.

    Args:
        host: Host do servidor SERPRO
        port: Porta do servidor SERPRO

    Returns:
        str: Caminho local do certificado recuperado e validado
    """
    content = _fetch_serpro_certificate_chain(host, port)
    crt_local_path = _write_serpro_certificate(content)

    context = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(crt_local_path)
    with _socket.create_connection((host, port), timeout=10) as sock:
        with context.wrap_socket(sock, server_hostname=host):
            pass

    _create_serpro_certificate_artifact(content)
    return crt_local_path


def _get_serpro_connection():
    """
    Cria conexão com o banco de dados SERPRO via Impala.

    Returns:
        Connection: Objeto de conexão com o banco
    """
    host = os.environ["radar_serpro_v2_host"]
    port = int(os.environ["radar_serpro_v2_port"])
    crt_local_path = _write_serpro_certificate(os.environ["radar_serpro_v2_crt"])

    # TCP diagnostic
    try:
        s = _socket.create_connection((host, port), timeout=10)
        print(f"TCP OK: {host}:{port}")
        s.close()
    except Exception as e:
        raise RuntimeError(f"TCP falhou: {type(e).__name__}: {e}") from e

    # SSL diagnostic
    try:
        ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(crt_local_path)
        s = _socket.create_connection((host, port), timeout=10)
        ss = ctx.wrap_socket(s, server_hostname=host)
        print(f"SSL OK: {ss.version()}, cert={ss.getpeercert()}")
        ss.close()
    except _ssl.SSLCertVerificationError as e:
        print(f"SSL falhou por certificado: {e}. Recuperando cadeia atual do SERPRO...")
        crt_local_path = _recover_serpro_certificate(host, port)
        print("SSL OK com cadeia recuperada do SERPRO")
    except Exception as e:
        raise RuntimeError(f"SSL falhou: {type(e).__name__}: {e}") from e

    conn = connect(
        host=host,
        port=port,
        user=os.environ["radar_serpro_v2_user"],
        password=os.environ["radar_serpro_v2_password"],
        auth_mechanism="LDAP",
        use_ssl=True,
        ca_cert=crt_local_path,
        database=os.environ["radar_serpro_v2_database"],
    )
    return conn


def extract_serpro_data(
    raw_filepath: str,
    timestamp: datetime,
) -> list[str]:
    """
    Extrai dados do SERPRO e salva localmente.

    Args:
        raw_filepath: Template do caminho para salvar o arquivo raw
        timestamp: Timestamp da captura para filtrar os dados

    Returns:
        list[str]: Lista com o caminho do arquivo salvo
    """
    print(f"Iniciando extração de dados do SERPRO para {timestamp.date()}")

    page_size = SERPRO_CAPTURE_PARAMS["page_size"]
    query = SERPRO_CAPTURE_PARAMS["query"].format(data=timestamp.strftime("%Y-%m-%d"))
    print(f"Executando query:\n{query}")

    filepaths = []

    with _get_serpro_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]

            page = 0
            while True:
                rows = cursor.fetchmany(page_size)
                if not rows:
                    break

                filepath = raw_filepath.format(page=page)
                df = pd.DataFrame(
                    rows, columns=columns, **SERPRO_CAPTURE_PARAMS["pre_treatment_reader_args"]
                )
                save_local_file(filepath=filepath, filetype="csv", data=df)
                filepaths.append(filepath)

                print(
                    f"""
                    Page size: {page_size}
                    Current page: {page}
                    Current page returned {len(rows)} rows"""
                )
                page += 1

    print(f"Extração concluída. Total de arquivos: {len(filepaths)}")
    return filepaths


@task(cache_policy=NO_CACHE)
def create_serpro_extractor(context: SourceCaptureContext):
    """
    Cria função de extração para o SERPRO.

    Args:
        context: Contexto da captura contendo informações do source e timestamp

    Returns:
        Callable: Função parcial configurada para extração
    """
    return partial(
        extract_serpro_data,
        raw_filepath=context.raw_filepath,
        timestamp=context.timestamp,
    )
