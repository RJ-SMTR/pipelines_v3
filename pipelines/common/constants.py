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

WEBHOOKS_SECRET_PATH = "webhooks"
OWNERS_DISCORD_MENTIONS = {
    "pipeliners": {
        "user_id": "962067746651275304",
        "type": "role",
    },
    "caio": {
        "user_id": "276427674002522112",
        "type": "user_nickname",
    },
    "rodrigo": {
        "user_id": "1031636163804545094",
        "type": "user_nickname",
    },
    "boris": {
        "user_id": "1109195532884262934",
        "type": "user_nickname",
    },
    "rafaelpinheiro": {
        "user_id": "1131538976101109772",
        "type": "user_nickname",
    },
    "dados_smtr": {
        "user_id": "1056928259700445245",
        "type": "role",
    },
    "devs_smtr": {
        "user_id": "1118274986461888612",
        "type": "role",
    },
}
