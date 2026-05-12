from pipelines.treatment__veiculo_dia.flow import treatment__veiculo_dia

treatment__veiculo_dia(datetime_start="2026-02-01", datetime_end="2026-02-15")

# -*- coding: utf-8 -*-
# # import os

# from queries.dev.utils import run_dbt, run_dbt_selector, run_dbt_tests, run_dbt_model

# # Veja os parâmetros disponíveis da função run_dbt_model em util.py

# import pandas as pd

# # inicio = "2025-07-01"
# # fim = "2025-07-31"
# # # Criar range de datas
# inicio = "2025-11-01"
# fim = "2025-11-01"
# # Criar range de datas
# run_dates = pd.date_range(start=inicio, end=fim, freq="D")
# # Montar string de partitions
# partitions = ", ".join([f"date({dt.year}, {dt.month}, {dt.day})" for dt in run_dates])
# # # Variáveis para passar ao dbt
# vars = {
#     "start_date": inicio,
#     "end_date": fim,
#     "date_range_start": f"{inicio}T00:00:00",
#     "date_range_end": f"{fim}T23:59:59",
#     "partitions": partitions,
#     }

# # run_dbt_model(
# # dataset_id="aux_calendario_manual",
# # _vars= vars,
# # flags = "--target hmg --defer --state target-base --favor-state")
# # run_dbt_selector(
# # selector_name="planejamento_diario",
# # _vars= vars,
# # flags = "--target hmg --defer --state target-base --favor-state ")

# # for date in run_dates:

# #     run_dbt_model(
# #     dataset_id="subsidio_data_versao_efetiva",
# #     _vars=vars,
# #     flags = "--target hmg --defer --state target-base --favor-state ")

# # for date in run_dates:

    
# #     run_dbt_model(
# #     _vars = {"run_date": date.strftime("%Y-%m-%d")},
# #     dataset_id="subsidio_data_versao_efetiva viagem_completa",
# #     upstream= True,
# #     exclude= "+gps_sppo +gtfs +subsidio_shapes_geom",
# #     flags = "--target hmg --defer --state target-base --favor-state ")

# # from queries.dev.utils import run_dbt, run_dbt_selector, run_dbt_tests

# # # Veja os parâmetros disponíveis da função run_dbt_model em util.py

# # run_dbt_tests(
# #     dataset_id="sppo_registros sppo_realocacao check_gps_treatment__gps_sppo sppo_veiculo_dia transacao transacao_riocard gps_validador",
# #       _vars=vars, 
# #     flags = "--target hmg --defer --state target-base ")

# # run_dbt_tests(
# #     dataset_id="tecnologia_servico viagem_planejada  ",
# #       _vars=vars, 
# #     flags = "--target hmg")

# # run_dbt_selector(
# #     selector_name="monitoramento_veiculo", _vars=vars,
# #     flags = "--target hmg")

# # run_dbt_selector(
# #     selector_name="monitoramento_temperatura", _vars=vars,
# #     flags = "--target hmg")


# # run_dbt_selector(
# #     selector_name="apuracao_subsidio_v9", _vars=vars,
# #     flags = "--target hmg")


# # run_dbt_tests(
# #     dataset_id="viagem_classificada viagem_regularidade_temperatura viagens_remuneradas sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem",
# #       _vars=vars, 
# #     flags = "--target hmg")
# -*- coding: utf-8 -*-
# import os

# import pandas as pd
# from queries.dev.utils import run_dbt_model, run_dbt_selector, run_dbt_tests, run_dbt

# # Veja os parâmetros disponíveis da função run_dbt_model em util.py

# inicio = "2026-04-16"
# fim = "2026-04-30"
# run_dates = pd.date_range(start=inicio, end=fim, freq="D")
# run_dates_list = run_dates.strftime("%Y-%m-%d").tolist()
# partitions = ", ".join([f"date({dt.year}, {dt.month}, {dt.day})" for dt in run_dates])
# vars = {
#     "start_date": inicio,
#     "end_date": fim,
#     "date_range_start": inicio + "T00:00:00",
#     "date_range_end": fim + "T01:00:00",
#     # "partitions": partitions,
# }



# run_dbt_tests(
#     dataset_id="veiculo_dia",
#     _vars=vars,
#     flags="--target dev --defer --state target-base"
#     # flags="--target prod",
# )




# run_dbt_selector(
#     selector_name="monitoramento_veiculo",
#     _vars=vars,
#     flags="--target dev --defer --state target-base")

# run_dbt_model(
#     dataset_id="veiculo_dia",
#     _vars=vars,
#     # exclude="+licenciamento +infracao",
#     # flags="--target hmg",
#     flags="--target dev --defer --state target-base --favor-state")






# run_dbt_model(
# dataset_id="gtfs",
# exclude="calendario aux_calendario_manual viagem_planejada_planejamento \
#                      matriz_integracao tecnologia_servico aux_ordem_servico_faixa_horaria \
#                      servico_planejado_faixa_horaria",
# _vars= {"data_versao_gtfs": "2026-01-26"},
# flags = "--target hmg")


# run_dbt_model(
# dataset_id="gtfs planejamento",
# exclude="calendario aux_calendario_manual viagem_planejada_planejamento \
#                      matriz_integracao tecnologia_servico aux_ordem_servico_faixa_horaria \
#                      servico_planejado_faixa_horaria aux_segmento_shape segmento_shape ",
# _vars= {"data_versao_gtfs": "2026-04-15"},
# flags="--target dev --defer --state target-base --favor-state")



# run_dbt_model(
# dataset_id="gtfs",
# exclude="calendario aux_calendario_manual viagem_planejada_planejamento \
#                      matriz_integracao tecnologia_servico aux_ordem_servico_faixa_horaria \
#                      servico_planejado_faixa_horaria",
# _vars= {"data_versao_gtfs": "2026-04-15"},
# flags = "--target dev --defer --state target-base --favor-state")




# run_dbt_model(
#     dataset_id="ordem_servico_trips_shapes_gtfs",
#     # table_id="ordem_servico_diaria",
#     _vars={"data_versao_gtfs": "2026-01-26"},
#     # flags="--target hmg",
#     flags="--target hmg --defer --state target-base --favor-state",
# )


# run_dbt_model(
#     dataset_id="ordem_servico_trips_shapes_gtfs",
#     # table_id="ordem_servico_diaria",
#     _vars={"data_versao_gtfs": "2026-02-02"},
#     # flags="--target hmg",
#     flags="--target hmg --defer --state target-base --favor-state",
# )

# run_dbt_model(
#     dataset_id="ordem_servico_trips_shapes_gtfs",
#     # table_id="ordem_servico_diaria",
#     _vars={"data_versao_gtfs": "2026-02-11"},
#     # flags="--target hmg",
#     flags="--target hmg --defer --state target-base --favor-state",
# )




# run_dbt_tests(
#     dataset_id="ordem_servico_trips_shapes_gtfs",
#     _vars={"data_versao_gtfs": "2026-01-26"},
#     flags="--target hmg",
    # flags="--target prod",
# )



# for run_date in run_dates_list:
#     # print(run_date)
#     run_dbt_model(
#         dataset_id="subsidio_data_versao_efetiva",
#         # upstream=True,
#         # exclude="+gps_sppo +gtfs +ordem_servico_trips_shapes_gtfs subsidio_shapes_geom",
#         _vars={"run_date": run_date},
#         # flags="--target hmg",
#         flags="--target dev --defer --state target-base --favor-state",
#     )
# for run_date in run_dates_list:
#     # print(run_date)
#     run_dbt_model(
#         dataset_id="viagem_completa",
#         upstream=True,
#         exclude="+gps_sppo +gtfs +subsidio_shapes_geom",
#         _vars={"run_date": run_date},
#         # flags="--target hmg",
#         flags="--target dev --defer --state target-base --favor-state",
#     )


# for run_date in run_dates_list:
#     # print(run_date)
#     run_dbt_model(
#         dataset_id="subsidio_data_versao_efetiva",
#         # upstream=True,
#         # exclude="+gps_sppo +gtfs +subsidio_shapes_geom",
#         _vars={"run_date": run_date},
#         # flags="--target hmg",
#         flags="--target dev --defer --state target-base")



# for run_date in run_dates_list:
#     # print(run_date)
#     run_dbt_model(
#         dataset_id="subsidio_data_versao_efetiva",
#         # upstream=True,
#         # exclude="+gps_sppo +gtfs +ordem_servico_trips_shapes_gtfs subsidio_shapes_geom",
#         _vars={"run_date": run_date},
#         # flags="--target hmg",
#         flags="--target hmg --defer --state target-base",
#     )


# run_dbt_tests(
#     dataset_id="viagem_planejada",
#     _vars=vars,
#     flags="--target hmg  --defer --state target-base",
#     # flags="--target prod",
# )



# run_dbt_selector(
#     selector_name="planejamento_diario",
#     _vars={"start_date": "2026-02-01",
#     "end_date": "2026-02-15",
#     "date_range_start": "2026-02-01" + "T00:00:00",
#     "date_range_end": "2026-02-15" + "T01:00:00"},
#     flags="--target hmg --defer --state target-base",
# )



# run_dbt(
#     resource="model",
#     selector_name="monitoramento_temperatura",
#     _vars= {"start_date": "2026-02-01",
#     "end_date": "2026-02-15",
#     "date_range_start": "2026-02-01" + "T00:00:00",
#     "date_range_end": "2026-02-15" + "T01:00:00"},
#     flags="--target hmg --defer --state target-base",
#     # flags="--target dev --defer --state target-base",
# )

# run_dbt(
#     resource="model",
#     selector_name="apuracao_subsidio_v9",
#     _vars= {"start_date": "2026-02-01",
#     "end_date": "2026-02-15",
#     "date_range_start": "2026-02-01" + "T00:00:00",
#     "date_range_end": "2026-02-15" + "T01:00:00"},
#     flags="--target hmg",
#     # flags="--target dev",
# )
# run_dbt_selector(
#     selector_name="planejamento_diario",
#     _vars=vars,
#     flags="--target hmg",
# )
# run_dbt_selector(
#     selector_name="monitoramento_veiculo",
#     _vars=vars,
#     flags="--target hmg",
# )


# run_dbt_model(
#     dataset_id="viagem_classificada",
#     _vars={"start_date": "2026-02-12",
#     "end_date": "2026-02-12",
#     "date_range_start": "2026-02-12" + "T00:00:00",
#     "date_range_end": "2026-02-12" + "T01:00:00"},
#     # exclude="+licenciamento +infracao",
#     # flags="--target hmg",
#     flags="--target dev --defer --state target-base",
# )


# run_dbt_selector(
#     selector_name="monitoramento_temperatura",
#     _vars=vars,
#     flags="--target hmg --defer --state target-base --favor-state",
# )
# run_dbt(
#     resource="model",
#     selector_name="apuracao_subsidio_v9",
#     _vars=vars,
#     # flags="--target hmg",
#     flags="--target hmg --defer --state target-base --favor-state",
# )
# run_dbt_tests(
#     dataset_id="viagem_classificada viagem_regularidade_temperatura viagens_remuneradas sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem",
#     _vars=vars,
#     flags="--target dev",
#     exclude="aux_viagem_temperatura veiculo_regularidade_temperatura_dia",
#     # flags="--target prod",
# )
