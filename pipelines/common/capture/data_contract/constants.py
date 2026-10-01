# -*- coding: utf-8 -*-
"""
Valores constantes compartilhados para validação dos contratos de dados
"""

from pathlib import Path

CAPTURE_METADATA_COLUMNS = frozenset({"timestamp_captura"})
GITHUB_API = "https://api.github.com"
REQUEST_TIMEOUT = (10, 60)
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MANIFEST = ROOT / "queries" / "target" / "manifest.json"
