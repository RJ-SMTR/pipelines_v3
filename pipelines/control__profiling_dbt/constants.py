# -*- coding: utf-8 -*-
"""Constantes do pipeline de profiling/teste de download de queries"""

# Repositório de onde a pasta `queries` é baixada em runtime
GIT_REPO_URL = "https://github.com/RJ-SMTR/pipelines_v3.git"

# Ref (branch, tag ou commit) padrão a ser baixada
DEFAULT_GIT_REF = "master"

# Nome da variável de ambiente que contém o token do GitHub (opcional, para repo privado)
GITHUB_TOKEN_ENV = "GITHUB_TOKEN"

# Intervalo (segundos) entre amostras do profiler de recursos
PROFILE_SAMPLE_INTERVAL = 0.5
