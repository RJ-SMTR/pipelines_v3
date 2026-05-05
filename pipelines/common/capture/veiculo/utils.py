# -*- coding: utf-8 -*-
"""
Funções de pré-tratamento para captura de dados de veículos SPPO.
"""

import numpy as np
import pandas as pd

from pipelines.common.capture.default_capture.utils import SourceCaptureContext


def pre_treatment_sppo_licenciamento(
    data: pd.DataFrame,
    context: SourceCaptureContext,  # noqa: ARG001
) -> pd.DataFrame:
    """Pré-tratamento dos dados de licenciamento de veículos SPPO."""
    for col in data.columns[data.dtypes == "object"].to_list():
        data[col] = data[col].str.strip()

    for col in data.columns[data.columns.str.contains("indicador")].to_list():
        data[col] = data[col].map({"Sim": True, "Nao": False})

    data = data.dropna(subset=["id_veiculo"])

    data["indicador_ar_condicionado"] = data["tipo_veiculo"].map(
        lambda x: None if not isinstance(x, str) else bool("C/AR" in x.replace(" ", ""))
    )

    data["status"] = "Licenciado"
    return data


def pre_treatment_sppo_infracao(
    data: pd.DataFrame,
    context: SourceCaptureContext,  # noqa: ARG001
) -> pd.DataFrame:
    """Pré-tratamento dos dados de infrações de veículos SPPO."""
    for col in data.columns[data.dtypes == "object"].to_list():
        data[col] = data[col].str.strip().replace("", np.nan)

    data["valor"] = data["valor"].astype(str).str.replace(",", ".").astype(float)
    return data
