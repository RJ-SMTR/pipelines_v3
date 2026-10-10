# -*- coding: utf-8 -*-
"""
Valores constantes compartilhados para validação dos contratos de dados
"""

from pathlib import Path

CAPTURE_METADATA_COLUMNS = {"data", "hora", "timestamp_captura", "_datetime_execucao_flow"}
DBT_TEST_METRICS = {
    "not_null": "nullValues",
    "unique": "duplicateValues",
    "accepted_values": "invalidValues",
}
CHECK_DESCRIPTIONS = {
    "field_is_present": "A coluna `{field}` está presente no arquivo",
    "field_type": "A coluna `{field}` tem o tipo esperado",
    "field_primary_key_required": "Todos os valores da chave primária `{field}` não nulos",
    "field_primary_key_unique": "Todos os valores da chave primária `{field}` são únicos",
    "field_null_values": "Todos os valores da coluna `{field}` não nulos",
    "field_duplicate_values": "Todos os valores da coluna `{field}` são únicos",
    "field_invalid_values": "Todos os valores da coluna `{field}` estão entre os aceitos",
    "row_count": "Quantidade de linhas do arquivo conforme o contrato",
    "field_relationships": "Todos os valores da coluna `{field}` existem em `{reference}`",
}
METRIC_LABELS = {
    "row_count": "quantidade_linhas",
    "missing_count": "quantidade_nulos",
    "duplicate_count": "quantidade_duplicados",
    "invalid_count": "quantidade_invalidos",
    "missing_reference_count": "quantidade_sem_referencia",
}
GITHUB_API = "https://api.github.com"
REQUEST_TIMEOUT = (10, 60)
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MANIFEST = ROOT / "queries" / "target" / "manifest.json"
