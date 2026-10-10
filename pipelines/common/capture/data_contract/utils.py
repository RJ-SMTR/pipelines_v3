# -*- coding: utf-8 -*-
"""Funções auxiliares para geração e validação de contratos ODCS."""

import json
import os
import re
from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from textwrap import wrap
from typing import Any
from urllib.parse import quote

import requests
import yaml
from datacontract.data_contract import DataContract
from datacontract.model.run import Run

from pipelines.common.capture.data_contract.constants import (
    CAPTURE_METADATA_COLUMNS,
    CHECK_DESCRIPTIONS,
    DEFAULT_MANIFEST,
    GITHUB_API,
    METRIC_LABELS,
    REQUEST_TIMEOUT,
    ROOT,
)
from pipelines.common.capture.data_contract.dbt_importer import import_contract_from_manifest
from pipelines.common.utils.utils import is_running_locally

RESULT_LABELS = {
    "passed": "aprovado",
    "failed": "reprovado",
    "warning": "alerta",
    "skipped": "ignorado",
    "error": "erro",
}

REASON_PATTERN = re.compile(
    r"^Actual (?P<metric>.+?) was (?P<actual>.+?), expected (?P<expected>.+)$", re.S
)
METRIC_PATTERN = re.compile(r"^(?P<name>\w+)(?=\()")


def translate_reason(reason: Any) -> str | None:
    """Traduz o motivo padrão retornado pelo datacontract para português.

    Args:
        reason (Any): Motivo retornado pelo check.

    Returns:
        str | None: Motivo traduzido, ou o original quando não segue o padrão conhecido.
            Métricas fora de METRIC_LABELS mantêm o nome original.
    """
    if reason is None:
        return None
    match = REASON_PATTERN.match(str(reason))
    if match is None:
        return str(reason)
    metric = METRIC_PATTERN.sub(
        lambda name: METRIC_LABELS.get(name["name"], name["name"]), match["metric"]
    )
    return f"Obtido {metric} = {match['actual']}, esperado {match['expected']}"


def _result_value(result: Any) -> str:
    """Retorna o valor textual de um resultado, inclusive quando ele é um enum.

    Args:
        result (Any): Resultado retornado pelo datacontract.

    Returns:
        str: Valor textual do resultado.
    """
    return getattr(result, "value", str(result))


def to_field(run: Run, check: Any) -> str | None:
    """Retorna o campo do check, qualificando-o quando há vários modelos.

    Args:
        run (Run): Resultado do teste do contrato.
        check (Any): Check cujo campo será exibido.

    Returns:
        str | None: Campo simples ou qualificado pelo nome do modelo.
    """
    models = [item.model for item in run.checks]
    if len(set(models)) > 1:
        if check.field is None:
            return check.model
        return f"{check.model}.{check.field}"
    return check.field


def check_description(check: Any) -> str:
    """Retorna a descrição em português do check.

    Usa a description da regra de qualidade que originou o check e, na ausência, uma
    descrição padrão pelo tipo do check. Sem correspondência, mantém o nome do datacontract.
    Em relationships, a coluna referenciada vem do nome do check, único lugar do resultado
    que a informa.

    Args:
        check (Any): Check do resultado do teste do contrato.

    Returns:
        str: Descrição do check.
    """
    if check.qualityDefinition:
        rule = yaml.safe_load(check.qualityDefinition) or {}
        if rule.get("description"):
            return rule["description"]
    template = CHECK_DESCRIPTIONS.get(check.type)
    if template is not None and check.field is not None:
        reference = (check.name or "").partition(" missing from ")[2]
        return template.format(field=check.field, reference=reference)
    return check.name


def contract_test_results(run: Run, table: str) -> list[dict]:
    """Converte os checks do contrato no formato usado nas notificações de testes.

    Args:
        run (Run): Resultado do teste do contrato.
        table (str): Nome exibido para agrupar os checks na notificação.

    Returns:
        list[dict]: Checks executados, com as chaves table, description e result
            (PASS, WARN, FAIL ou ERROR).
    """
    results = {"passed": "PASS", "warning": "WARN", "failed": "FAIL"}
    return [
        {
            "table": table,
            "description": check_description(check),
            "result": results.get(_result_value(check.result), "ERROR"),
        }
        for check in run.checks
        if _result_value(check.result) != "skipped"
    ]


def _format_table_border(widths: tuple[int, ...]) -> str:
    """Cria a borda ASCII usada no início, meio e fim da tabela.

    Args:
        widths (tuple[int, ...]): Largura de cada coluna.

    Returns:
        str: Linha de borda com a largura das colunas informada.
    """
    return "+" + "+".join("-" * (width + 2) for width in widths) + "+"


def _wrap_cell(value: Any, width: int) -> list[str]:
    """Quebra o conteúdo de uma célula na largura definida para a tabela.

    Args:
        value (Any): Valor que será exibido na célula.
        width (int): Largura máxima da célula.

    Returns:
        list[str]: Linhas da célula já ajustadas à largura informada.
    """
    text = "" if value is None else str(value)
    lines = []
    for line in text.splitlines() or [""]:
        lines.extend(wrap(line, width=width, break_long_words=True, break_on_hyphens=False) or [""])
    return lines


def _format_table_row(values: list[Any], widths: tuple[int, ...]) -> list[str]:
    """Formata uma linha textual, preservando o alinhamento entre as células.

    Args:
        values (list[Any]): Valores das células da linha.
        widths (tuple[int, ...]): Largura de cada coluna.

    Returns:
        list[str]: Linhas da tabela correspondentes à linha formatada.
    """
    wrapped_values = [_wrap_cell(value, width) for value, width in zip(values, widths, strict=True)]
    return [
        "| "
        + " | ".join(
            (cell_lines[index] if index < len(cell_lines) else "").ljust(width)
            for cell_lines, width in zip(wrapped_values, widths, strict=True)
        )
        + " |"
        for index in range(max(map(len, wrapped_values)))
    ]


def _format_test_results_table(run: Run) -> str:
    """Formata os checks em uma tabela ASCII de largura controlada.

    Args:
        run (Run): Resultado do teste do contrato.

    Returns:
        str: Tabela textual dos checks, sem renderização Rich.
    """
    widths = (10, 28, 15, 22)
    border = _format_table_border(widths)
    lines = [
        border,
        *_format_table_row(["Resultado", "Verificação", "Campo", "Detalhes"], widths),
        border,
    ]
    checks = sorted(
        run.checks,
        key=lambda item: (
            _result_value(item.result),
            item.model or "",
            item.field or "",
        ),
    )
    for check in checks:
        lines.extend(
            _format_table_row(
                [
                    RESULT_LABELS.get(_result_value(check.result), _result_value(check.result)),
                    check_description(check),
                    to_field(run, check),
                    translate_reason(check.reason),
                ],
                widths,
            )
        )
    lines.append(border)
    return "\n".join(lines)


def _format_failed_checks(run: Run, width: int = 88) -> list[str]:
    """Formata os checks reprovados e as amostras coletadas.

    Args:
        run (Run): Resultado do teste do contrato.
        width (int): Largura máxima das linhas do resumo.

    Returns:
        list[str]: Linhas com os motivos e as amostras dos checks reprovados.
    """
    lines = []
    position = 1
    for check in run.checks:
        if _result_value(check.result) in ("passed", "skipped"):
            continue
        prefix = f"{position}) {check_description(check)}: "
        reason_lines = wrap(
            prefix + (translate_reason(check.reason) or ""),
            width=width,
            subsequent_indent="   ",
            break_long_words=True,
            break_on_hyphens=False,
        )
        lines.extend(reason_lines or [prefix.rstrip()])
        if check.failedSamples:
            lines.append("   Amostras com falha:")
            for sample in check.failedSamples:
                sample_json = json.dumps(sample, ensure_ascii=False, default=str)
                lines.extend(
                    wrap(
                        f"   - {sample_json}",
                        width=width,
                        subsequent_indent="     ",
                        break_long_words=True,
                        break_on_hyphens=False,
                    )
                )
        position += 1
    return lines


def _format_test_results_summary(run: Run) -> list[str]:
    """Formata o resumo textual do resultado do contrato.

    Args:
        run (Run): Resultado do teste do contrato.

    Returns:
        list[str]: Linhas do resumo, incluindo erros e amostras quando existirem.
    """
    if _result_value(run.result) == "passed":
        skipped = sum(1 for check in run.checks if _result_value(check.result) == "skipped")
        skipped_info = f" ({skipped} ignoradas)" if skipped else ""
        duration = (run.timestampEnd - run.timestampStart).total_seconds()
        return [
            "🟢 Contrato de dados válido. "
            f"{len(run.checks)} verificações executadas{skipped_info} em {duration} segundos."
        ]
    if _result_value(run.result) == "skipped":
        return ["🔵 Validação do contrato de dados ignorada"]
    if _result_value(run.result) == "warning":
        return [
            "🟠 Contrato de dados com alertas:",
            *_format_failed_checks(run),
        ]
    return ["🔴 Contrato de dados inválido. Erros encontrados:", *_format_failed_checks(run)]


def format_test_results(run: Run) -> str:
    """Formata tabela, sumário e amostras para impressão direta no log.

    A saída usa somente texto ASCII na tabela e quebras de linha controladas, evitando
    a renderização Rich e a quebra estrutural da tabela na interface de logs do Prefect.

    Args:
        run (Run): Resultado do teste do contrato.

    Returns:
        str: Texto com largura controlada para ser escrito diretamente no log.
    """
    return "\n".join([_format_test_results_table(run), *_format_test_results_summary(run)])


def _github_get(
    session: requests.Session,
    repository: str,
    endpoint: str,
    **request_options: Any,
) -> requests.Response:
    """
    Consulta um endpoint da API do GitHub.

    Args:
        session (requests.Session): Sessão HTTP usada na requisição.
        repository (str): Repositório no formato `owner/name`.
        endpoint (str): Caminho do endpoint relativo ao repositório.
        request_options (Any): Opções adicionais encaminhadas para `Session.get`.

    Returns:
        requests.Response: Resposta HTTP da API do GitHub.

    Raises:
        requests.HTTPError: A resposta da API indica uma falha HTTP.
    """
    response = session.get(
        f"{GITHUB_API}/repos/{repository}/{endpoint}",
        timeout=REQUEST_TIMEOUT,
        **request_options,
    )
    response.raise_for_status()
    return response


def download_contracts_from_commit(env: str) -> Path:
    """
    Disponibiliza os contratos no diretório compartilhado do flow.

    Args:
        env (str): Ambiente usado para selecionar a branch em execução remota.

    Returns:
        Path: Diretório com os contratos ODCS.

    Raises:
        ValueError: Árvore do repositório incompleta.
        KeyError: GIT_BRANCH ausente em execução remota não produtiva.
        requests.HTTPError: Falha na consulta ao GitHub.
    """
    contracts_dir = ROOT / "contracts"
    if is_running_locally():
        return contracts_dir

    repository = "RJ-SMTR/pipelines_v3"
    ref = "master" if env == "prod" else os.environ["GIT_BRANCH"]

    with requests.Session() as session:
        session.headers.update({"Accept": "application/vnd.github+json"})
        sha = _github_get(session, repository, f"commits/{quote(ref, safe='')}").json()["sha"]

        tree = _github_get(
            session,
            repository,
            f"git/trees/{sha}",
            params={"recursive": "1"},
        ).json()
        if tree.get("truncated"):
            raise ValueError("GitHub retornou uma árvore incompleta para descobrir contratos.")
        contract_paths = [
            item["path"]
            for item in tree["tree"]
            if item["type"] == "blob"
            and item["path"].startswith("contracts/")
            and item["path"].endswith(".odcs.yaml")
        ]

        contracts_dir.mkdir(parents=True, exist_ok=True)
        for relative_path in sorted(set(contract_paths)):
            path = ROOT / relative_path
            content = _github_get(
                session,
                repository,
                f"contents/{quote(relative_path, safe='/')}",
                params={"ref": sha},
                headers={"Accept": "application/vnd.github.raw+json"},
            ).content
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
    return contracts_dir


def adapt_contract_schema(
    contract: dict[str, Any],
    ignored_columns: Iterable[str] = (),
    primary_keys: Iterable[str] | None = (),
) -> dict[str, Any]:
    """
    Remove colunas ausentes no bruto e aplica as chaves primárias da fonte.

    Args:
        contract (dict[str, Any]): Contrato importado do dbt.
        ignored_columns (Iterable[str]): Colunas que não existem no bruto.
        primary_keys (Iterable[str] | None): Chaves primárias configuradas na fonte.

    Returns:
        dict[str, Any]: Cópia do contrato com colunas e chaves ajustadas.

    Raises:
        ValueError: Uma chave primária não existe entre as colunas do contrato.
    """
    ignored = CAPTURE_METADATA_COLUMNS | set(ignored_columns)
    keys = list(primary_keys or ())
    adapted_contract = deepcopy(contract)

    for schema in adapted_contract.get("schema", []):
        properties = schema.get("properties", [])
        schema["properties"] = [
            property_ for property_ in properties if property_.get("name") not in ignored
        ]
        properties_by_name = {
            property_.get("name"): property_ for property_ in schema["properties"]
        }
        missing_keys = [key for key in keys if key not in properties_by_name]
        if missing_keys:
            raise ValueError(
                f"Chaves primárias não encontradas no contrato "
                f"{schema.get('name', '<sem nome>')}: {', '.join(missing_keys)}"
            )
        for property_ in schema["properties"]:
            property_.pop("primaryKey", None)
            property_.pop("primaryKeyPosition", None)
        custom_properties = schema.get("customProperties")
        if isinstance(custom_properties, list):
            schema["customProperties"] = [
                item for item in custom_properties if item.get("property") != "primaryKey"
            ]
        for position, key in enumerate(keys, start=1):
            properties_by_name[key]["primaryKey"] = True
            properties_by_name[key]["primaryKeyPosition"] = position

    return adapted_contract


def add_local_server(
    contract: dict[str, Any],
    raw_filepath: str,
    file_format: str = "csv",
    server_name: str = "incoming",
) -> dict[str, Any]:
    """
    Cria uma cópia do contrato para validar o arquivo bruto localmente.

    Args:
        contract (dict[str, Any]): Contrato versionado.
        raw_filepath (str): Caminho do arquivo bruto.
        file_format (str): Formato do arquivo bruto.
        server_name (str): Nome do servidor local.

    Returns:
        dict[str, Any]: Cópia com servidor local e nomes usados pelo executor.
    """
    adapted_contract = deepcopy(contract)
    for schema in adapted_contract["schema"]:
        schema["physicalName"] = schema["name"]
    adapted_contract["servers"] = [
        {"server": server_name, "type": "local", "path": raw_filepath, "format": file_format}
    ]
    return adapted_contract


def generate_contracts(
    *,
    manifest_path: Path = DEFAULT_MANIFEST,
    contracts_dir: Path = ROOT / "contracts",
) -> None:
    """
    Gera contratos para modelos staging_ com datacontract_cli nos metadados.

    Args:
        manifest_path (Path): Caminho do manifest gerado pelo dbt Core.
        contracts_dir (Path): Diretório dos contratos gerados.

    Raises:
        FileNotFoundError: Manifest dbt não encontrado.
        ValueError: Contrato inválido.
    """
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Manifest dbt não encontrado em {manifest_path}; execute dbt parse primeiro"
        )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    generated_paths = set()
    for model in sorted(manifest["nodes"].values(), key=lambda node: node["name"]):
        if model["resource_type"] != "model" or not model["name"].startswith("staging_"):
            continue

        columns = model["columns"]
        nodes_to_check = [model]
        nodes_to_check.extend(columns.values())
        has_contract = False
        for node in nodes_to_check:
            config = node.get("config") or {}
            metadata = config.get("meta") or node.get("meta") or {}
            if "datacontract_cli" in metadata:
                has_contract = True
                break

        if not has_contract:
            continue

        model_name = model["name"]
        path = Path(model["schema"]) / f"{model_name}.odcs.yaml"
        contract = import_contract_from_manifest(manifest, model)
        contract.update(
            apiVersion="v3.1.0",
            id=f"urn:datacontract:{path.parts[0]}:{model_name}",
            name=f"{path.parts[0]}/{model_name}",
            status="active",
        )
        expected = yaml.safe_dump(contract, sort_keys=False, allow_unicode=True)
        result = DataContract(data_contract_str=expected).lint()
        if not result.has_passed():
            raise ValueError(f"Contrato inválido para {model_name}: {result.model_dump_json()}")
        output = contracts_dir / path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(expected, encoding="utf-8")
        generated_paths.add(path)
        print(f"Contrato gerado: {output}")

    for output in sorted(contracts_dir.rglob("*.odcs.yaml")):
        if output.relative_to(contracts_dir) not in generated_paths:
            output.unlink()
            print(f"Contrato obsoleto removido: {output}")
