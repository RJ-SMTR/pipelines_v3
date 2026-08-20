# -*- coding: utf-8 -*-
# /// script
# dependencies = [
#     "google-cloud-bigquery>=3.29.0",
#     "pyyaml>=6.0.2",
# ]
# ///

import json
import os
from pathlib import Path

import yaml
from google.cloud import bigquery

# Magic number constants
MAX_TABLE_DESCRIPTION_LENGTH = 16384
MAX_COLUMN_DESCRIPTION_LENGTH = 1024


def extrair_descricoes_de_modelos(manifest):
    tabelas = []

    for _node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model":
            schema = node.get("schema")
            table_name = node.get("alias") or node.get("name")
            descricao = node.get("description", "").strip()
            column_descriptions = {}

            for col_name, col_data in node.get("columns", {}).items():
                desc = col_data.get("description", "").strip()
                if desc:
                    column_descriptions[col_name] = desc

            tabelas.append(
                {
                    "schema": schema,
                    "table_name": table_name,
                    "description": descricao,
                    "column_descriptions": column_descriptions,
                }
            )

    return tabelas


# A função é recursiva para atualizar campos aninhados, i.e. json
def atualizar_descricoes_campos(fields, prefixo, descricoes_colunas, table_id):
    alterado = False

    for field in fields:
        nome_completo = f"{prefixo}.{field['name']}" if prefixo else field["name"]
        descricao_campo = descricoes_colunas.get(nome_completo)

        if descricao_campo and (
            ("description" in field and field["description"] != descricao_campo)
            or "description" not in field
        ):
            if len(descricao_campo) > MAX_COLUMN_DESCRIPTION_LENGTH:
                print(
                    f"A descrição da coluna '{nome_completo}' da tabela '{table_id}' "
                    f"tem mais de {MAX_COLUMN_DESCRIPTION_LENGTH} caracteres, "
                    f"não é possível atualizar."
                )
            else:
                print(f"Atualizando a descrição da coluna '{nome_completo}' da tabela '{table_id}'")
                field["description"] = descricao_campo
                alterado = True

        subcampos = field.get("fields")
        # caso base: campo escalar, sem subcampos para recursão
        if not subcampos:
            continue

        if atualizar_descricoes_campos(subcampos, nome_completo, descricoes_colunas, table_id):
            alterado = True

    return alterado


def atualizar_descricao_tabela(  # noqa: PLR0913
    client,
    projeto,
    schema,
    nome,
    descricao_tabela,
    descricoes_colunas,
    table_cache,
    dry_run=False,
):
    table_id = f"{projeto}.{schema}.{nome}"
    table_def = table_cache.get(table_id)
    if table_def is None:
        table_def = client.get_table(table_id)
    tabela = table_def.to_api_repr()
    alterado = False

    if descricao_tabela and tabela.get("description") != descricao_tabela:
        if len(descricao_tabela) > MAX_TABLE_DESCRIPTION_LENGTH:
            print(
                f"A descrição da tabela '{table_id}' tem mais de "
                f"{MAX_TABLE_DESCRIPTION_LENGTH} caracteres, não é possível atualizar."
            )
        else:
            print(f"Atualizando a descrição da tabela '{table_id}'")
            tabela["description"] = descricao_tabela
            alterado = True

    if atualizar_descricoes_campos(tabela["schema"]["fields"], "", descricoes_colunas, table_id):
        alterado = True

    if not alterado:
        table_cache[table_id] = table_def
        return

    tabela = table_def.from_api_repr(tabela)
    if dry_run:
        print(f"[DRY_RUN] {table_id}: alteração não aplicada")
        table_cache[table_id] = table_def
    else:
        table_cache[table_id] = client.update_table(tabela, ["description", "schema"])


def propagate_labels(  # noqa: PLR0912, PLR0915
    manifest, client, table_cache, dry_run=False
):
    allowed_resource_types = {"model", "source"}

    with Path("queries/tag_propagation_allowlist.yml").open("r", encoding="utf-8") as f:
        allowlist_yaml = yaml.safe_load(f)

    allowlist = set(allowlist_yaml.get("tags-allowlist", []))

    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    tags_by_node = {}
    for dct in [nodes, sources]:
        for node, data in dct.items():
            if data["resource_type"] not in allowed_resource_types:
                continue
            if "tags" in data:
                filtered_tags = {t for t in data["tags"] if t in allowlist}
                tags_by_node[node] = filtered_tags
            else:
                tags_by_node[node] = set()

    def dfs(node, inherited_tags):
        if node in sources:
            return

        for dep in nodes[node].get("depends_on", {}).get("nodes", []):
            if dep not in tags_by_node:
                continue
            before = tags_by_node[dep].copy()
            tags_by_node[dep].update({t for t in inherited_tags if t in allowlist})

            if tags_by_node[dep] != before:
                dfs(dep, tags_by_node[dep])

    for node, _ in tags_by_node.items():
        dfs(node, tags_by_node[node])

    for node, tags in tags_by_node.items():
        if node in nodes:
            data = nodes[node]
        else:
            data = sources[node]

        if data.get("config", {}).get("materialized") == "ephemeral":
            continue
        if "intermediate" in node.lower():
            continue

        database = data.get("database")
        schema = data.get("schema")
        table_name = data.get("alias") or data.get("name")

        if not tags:
            continue

        if not (database and schema and table_name):
            continue

        if database != "rj-smtr":
            continue

        full_id = f"{database}.{schema}.{table_name}"

        table = table_cache.get(full_id)
        if table is None:
            try:
                table = client.get_table(full_id)
            except Exception as e:
                print(f"{full_id} não encontrada: {e}")
                continue

        labels = table.labels or {}
        for t in tags:
            labels[t] = "true"
        table.labels = labels

        if dry_run:
            print(f"[DRY_RUN] Atualizaria {full_id} com tags: {sorted(tags)}")
            continue

        try:
            table_cache[full_id] = client.update_table(table, ["labels"])
            print(f"Atualizado {full_id} com tags: {sorted(tags)}")
        except Exception as e:
            print(f"Erro atualizando {full_id}: {e}")


def main():
    dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    credentials = json.loads(os.getenv("GKE_SA_KEY"))
    client = bigquery.Client.from_service_account_info(credentials, project="rj-smtr")
    with Path("queries/target/manifest.json").open("r") as f:
        manifest = json.load(f)
    modelos = extrair_descricoes_de_modelos(manifest)
    table_cache = {}

    for modelo in modelos:
        try:
            atualizar_descricao_tabela(
                client,
                "rj-smtr",
                modelo["schema"],
                modelo["table_name"],
                modelo["description"],
                modelo["column_descriptions"],
                table_cache,
                dry_run=dry_run,
            )
        except Exception as e:
            print(f"Error: {modelo['table_name']}: {e}")

    propagate_labels(manifest, client, table_cache, dry_run=dry_run)


if __name__ == "__main__":
    main()
