# -*- coding: utf-8 -*-
"""Funções de captura dos dados do RDO"""

import pandas as pd

from pipelines.capture__rdo_geral import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext


def rename_rdo_columns(
    data: pd.DataFrame,
    context: SourceCaptureContext,
) -> pd.DataFrame:
    info = context.source.table_id.split("_")
    transport_mode = info[2].upper()
    report_type = info[0].upper()
    reindex_columns = constants.RDO_REINDEX_COLUMNS[transport_mode][report_type]
    data.columns = reindex_columns[: len(data.columns)]
    return data
