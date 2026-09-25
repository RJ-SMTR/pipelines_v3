# -*- coding: utf-8 -*-
"""Obtém contratos versionados no GitHub com um único commit por execução."""

import hashlib
import os
import re
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import yaml

DEFAULT_REPOSITORY = "RJ-SMTR/pipelines_v3"
GITHUB_API = "https://api.github.com"
REQUEST_TIMEOUT = (10, 60)


def contract_relative_path(source_name: str, model_name: str) -> str:
    """Retorna o caminho do artefato, sem aceitar caminhos arbitrários."""
    for value in (source_name, model_name):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", value):
            raise ValueError(f"Nome inválido no caminho do contrato: {value!r}")
    return f"contracts/{source_name}/{model_name}.odcs.yaml"


def get_contract_ref(env: str) -> str:
    """Usa master em prod, a branch registrada no deploy em dev ou o checkout local."""
    if env == "prod":
        return "master"
    branch = os.environ.get("GIT_BRANCH", "").strip()
    if branch:
        return branch
    result = subprocess.run(
        ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd=Path(__file__).resolve().parents[4],
        capture_output=True,
        text=True,
        check=False,
    )
    branch = result.stdout.strip()
    if result.returncode or not branch:
        raise ValueError(
            "Não foi possível identificar a branch: checkout ausente ou HEAD detached."
        )
    return branch


def download_contract_snapshot(paths: list[str], env: str) -> dict[str, Any]:
    """Resolve a referência uma vez e baixa todos os contratos desse mesmo SHA.

    Não há fallback para cache antigo. Falhas HTTP, contrato ausente ou YAML
    inválido interrompem a captura. O token opcional só é enviado à API GitHub.
    """
    if not paths:
        return {}
    repository = os.environ.get("DATA_CONTRACT_GITHUB_REPOSITORY", DEFAULT_REPOSITORY)
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("DATA_CONTRACT_GITHUB_REPOSITORY deve ser owner/repository.")
    with requests.Session() as session:
        session.headers.update({"Accept": "application/vnd.github+json"})
        token = os.environ.get("DATA_CONTRACT_GITHUB_TOKEN")
        if token:
            session.headers["Authorization"] = f"Bearer {token}"
        ref = get_contract_ref(env)
        response = session.get(
            f"{GITHUB_API}/repos/{repository}/commits/{quote(ref, safe='')}",
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        sha = response.json().get("sha", "")
        if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise ValueError("GitHub não retornou um SHA de commit válido para o contrato.")
        contracts = {}
        for path in sorted(set(paths)):
            response = session.get(
                f"{GITHUB_API}/repos/{repository}/contents/{quote(path, safe='/')}",
                params={"ref": sha},
                headers={"Accept": "application/vnd.github.raw+json"},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            content = response.content
            contract = yaml.safe_load(content)
            if (
                not isinstance(contract, dict)
                or contract.get("kind") != "DataContract"
                or not contract.get("schema")
            ):
                raise ValueError(f"Contrato ODCS inválido no repositório: {path}")
            if contract.get("servers"):
                raise ValueError(
                    f"O contrato versionado não deve conter servidores de execução: {path}"
                )
            digest = hashlib.sha256(content).hexdigest()
            contracts[path] = {"contract": contract, "sha256": digest}
            print(f"Contrato: {repository}@{sha} {path} sha256={digest}")
    return {"repository": repository, "ref": ref, "sha": sha, "contracts": contracts}
