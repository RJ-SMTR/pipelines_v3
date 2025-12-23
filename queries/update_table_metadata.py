# -*- coding: utf-8 -*-
import json
import os

import yaml
from google.cloud import bigquery


def extrair_descricoes_de_modelos(manifest):
    tabelas = []

    for node_id, node in manifest.get("nodes", {}).items():
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


def atualizar_descricao_tabela(client, projeto, schema, nome, descricao_tabela, descricoes_colunas):
    table_id = f"{projeto}.{schema}.{nome}"
    table_def = client.get_table(table_id)
    tabela = table_def.to_api_repr()

    if descricao_tabela and tabela["description"] != descricao_tabela:
        if len(descricao_tabela) > 16384:
            print(
                f"A descrição da da tabela '{table_id}' tem mais de 16384 caracteres, \
                    não é possível atualizar."
            )
        else:
            print(f"Atualizando a descrição da tabela '{table_id}'")
            tabela["description"] = descricao_tabela

    for i in range(len(tabela["schema"]["fields"])):
        field = tabela["schema"]["fields"][i]
        if field["name"] in descricoes_colunas and descricoes_colunas[field["name"]]:
            if (
                "description" in field and field["description"] != descricoes_colunas[field["name"]]
            ) or "description" not in field:
                if len(descricoes_colunas[field["name"]]) > 1024:
                    print(
                        f"A descrição da coluna '{field['name']}' da tabela '{table_id}' \
                            tem mais de 1024 caracteres, não é possível atualizar."
                    )
                    continue
                print(f"Atualizando a descrição da coluna '{field['name']}' da tabela '{table_id}'")
                tabela["schema"]["fields"][i]["description"] = descricoes_colunas[field["name"]]

    tabela = table_def.from_api_repr(tabela)
    client.update_table(tabela, ["description", "schema"])


def propagate_labels(manifest, client):
    ALLOWED_RESOURCE_TYPES = {"model", "source"}

    with open("queries/tag_propagation_allowlist.yml", "r", encoding="utf-8") as f:
        allowlist_yaml = yaml.safe_load(f)

    allowlist = set(allowlist_yaml.get("tags-allowlist", []))

    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    tags_by_node = {}
    for dct in [nodes, sources]:
        for node, data in dct.items():
            if data["resource_type"] not in ALLOWED_RESOURCE_TYPES:
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
            tags_by_node[dep].update(set(t for t in inherited_tags if t in allowlist))

            if tags_by_node[dep] != before:
                dfs(dep, tags_by_node[dep])

    for node in tags_by_node:
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

        try:
            table = client.get_table(full_id)
        except Exception as e:
            print(f"{full_id} não encontrada: {e}")
            continue

        labels = table.labels or {}
        for t in tags:
            labels[t] = "true"
        table.labels = labels

        try:
            client.update_table(table, ["labels"])
            print(f"Atualizado {full_id} com tags: {sorted(tags)}")
        except Exception as e:
            print(f"Erro atualizando {full_id}: {e}")


def main():
    credentials = json.loads(os.getenv("BQ_AUTH_SA"))
    client = bigquery.Client.from_service_account_info(credentials, project="rj-smtr")
    with open("queries/target/manifest.json", "r") as f:
        manifest = json.load(f)
    modelos = extrair_descricoes_de_modelos(manifest)

    for modelo in modelos:
        try:
            atualizar_descricao_tabela(
                client,
                "rj-smtr",
                modelo["schema"],
                modelo["table_name"],
                modelo["description"],
                modelo["column_descriptions"],
            )
        except Exception as e:
            print(f"Error: {modelo['table_name']}: {e}")

    propagate_labels(manifest, client)


if __name__ == "__main__":
    main()
