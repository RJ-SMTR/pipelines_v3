# Motivo da alteração — SourceTable/DBTSelector: config pura, IO atrás de um adapter

Candidato 2 do relatório de arquitetura gerado em 2026-07-13
(`/improve-codebase-architecture`, modelo Fable 5), scoped aos hot spots de
`common/` (capture/treatment builders), capturas riorotativo e o flow de
timestamps divergentes da Jaé.

## Problema identificado

`SourceTable.__init__` (`pipelines/common/utils/gcp/bigquery.py:172-236`) e
`DBTSelector._get_schedule_cron`
(`pipelines/common/treatment/default_treatment/utils.py:201-225`) faziam IO
de disco (leitura e parse de `prefect.yaml`) e inspeção da stack do chamador
(`inspect.stack()`) dentro do construtor. Isso tornava impossível construir
esses objetos em memória num teste unitário sem ter um `prefect.yaml`
corretamente formatado no disco — e a mesma lógica de parsing de YAML estava
duplicada nas duas classes.

## Solução aplicada

`SourceTable` e `DBTSelector` passaram a ser construtores puros — só
calculam `flow_folder_path`. A leitura de `prefect.yaml` foi extraída para
`pipelines/common/utils/deployment.py` (`get_flow_folder_path`,
`get_flow_schedule_cron`) e `schedule_cron` virou uma `@cached_property`,
lida sob demanda no primeiro acesso, não mais na construção do objeto.

## Ganhos

- Construção de `SourceTable`/`DBTSelector` não depende mais de arquivo em
  disco — desbloqueia testes unitários em memória para os módulos que os
  consomem.
- Elimina a duplicação do parser de YAML entre `bigquery.py` e
  `default_treatment/utils.py`.
- Custo de IO só é pago se e quando `schedule_cron` é de fato acessado
  (memoizado após o primeiro acesso via `cached_property`).
