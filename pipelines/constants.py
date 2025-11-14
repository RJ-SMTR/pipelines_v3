# -*- coding: utf-8 -*-
"""
Valores constantes gerais para pipelines da rj-smtr
"""

PROJECT_NAME = {"dev": "rj-smtr-dev", "prod": "rj-smtr-staging"}
DEFAULT_BUCKET_NAME = {"dev": "rj-smtr-dev", "prod": "rj-smtr-staging"}
TIMEZONE = "America/Sao_Paulo"

# RETRY POLICY #
MAX_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3
RETRY_DELAY = 10
