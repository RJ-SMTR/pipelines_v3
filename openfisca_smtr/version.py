# -*- coding: utf-8 -*-
"""Versao do pacote / regra normativa."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


def get_versao_regra() -> str:
    """Versao instalada de ``openfisca_smtr`` (fallback para pyproject)."""
    try:
        return version("openfisca_smtr")
    except PackageNotFoundError:
        return "0.1.0"
