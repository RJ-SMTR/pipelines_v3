{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        materialized="incremental",
        incremental_strategy="insert_overwrite",
    )
}}

with
    planilha_source as (
        /* A captura aninha as colunas em `content` (JSON) e mantém uma linha por captura;
           extraímos os campos e deduplicamos para a captura mais recente por `data`. */
        select
            safe.parse_date('%d/%m/%Y', trim(dia)) as data,
            safe_cast(json_value(content, '$.tipo_dia') as string) as tipo_dia,
            safe_cast(json_value(content, '$.subtipo_dia') as string) as subtipo_dia,
            safe_cast(json_value(content, '$.tipo_os') as string) as tipo_os
        from {{ source("source_smtr", "calendario_manual") }}
        qualify
            row_number() over (
                partition by safe.parse_date('%d/%m/%Y', trim(dia))
                order by timestamp_captura desc
            )
            = 1
    ),
    planilha as (
        /* A partir da data de corte, o calendário manual vem da planilha (source). */
        select
            data,
            date(null) as feed_start_date,
            nullif(trim(tipo_dia), '') as tipo_dia,
            nullif(trim(subtipo_dia), '') as subtipo_dia,
            nullif(trim(tipo_os), '') as tipo_os
        from planilha_source
        where
            data >= date("{{ var('data_inicio_calendario_sheets') }}")
            {% if is_incremental() %}
                and data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% endif %}
    ),
    historico as (
        /* Antes da data de corte, o histórico permanece congelado no hardcoded. */
        select
            data,
            date(null) as feed_start_date,
            case
                when data = "2024-10-21"
                then "Ponto Facultativo"  -- Ponto Facultativo - Dia do Comerciário - (Processo.Rio MTR-DES-2024/64171)
                when data = "2024-10-28"
                then "Ponto Facultativo"  -- Ponto Facultativo - Dia do Servidor Público - (Processo.Rio MTR-DES-2024/64417)
                when data between date(2024, 11, 18) and date(2024, 11, 19)
                then "Ponto Facultativo"  -- Ponto Facultativo - G20 - (Processo.Rio MTR-DES-2024/67477)
                when data = date(2025, 02, 28)
                then "Ponto Facultativo"  -- Ponto Facultativo - Sexta-feira de Carnaval - (Processo.Rio MTR-PRO-2025/03920)
                when data between date(2025, 03, 03) and date(2025, 03, 04)
                then "Domingo"  -- Carnaval - (Processo.Rio MTR-PRO-2025/03920)
                when data = date(2025, 04, 18)
                then "Domingo"  -- Feriado - Sexta Feira Santa [Portaria MGI nº 9.783/ 2024]
                when data = date(2025, 04, 22)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 55883/2025
                when data = date(2025, 05, 02)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 56034 DE 29 DE ABRIL DE 2025
                when data = date(2025, 06, 19)
                then "Domingo"  -- DECRETO RIO Nº 56189 DE 10 DE JUNHO DE 2025 / MTR-MEM-2025/01539
                when data = date(2025, 06, 20)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 56189 DE 10 DE JUNHO DE 2025 / MTR-MEM-2025/01539
                when data = date(2025, 07, 04)
                then "Ponto Facultativo"  -- LEI Nº 8.881, DE 14 DE ABRIL DE 2025 / MTR-PRO-2025/16278
                when data = date(2025, 07, 07)
                then "Ponto Facultativo"  -- LEI Nº 8.881, DE 14 DE ABRIL DE 2025 / MTR-PRO-2025/16278
                when data = date(2025, 10, 20)
                then "Ponto Facultativo"  -- MTR-MEM-2025/02734 - Indicação de tipo dia "Ponto Facultativo" para o feriado do dia do comerciário
                when data = date(2025, 10, 31)
                then "Ponto Facultativo"  -- Retificação Recurso SMTR202511005101
                when data = date(2025, 11, 21)
                then "Ponto Facultativo"  -- Decreto Rio nº 57.139, de 10 de novembro de 2025.
                when data = date(2025, 12, 24)
                then "Sabado"  -- 000399.001368/2025-25 - Véspera de Natal
                when data = date(2025, 12, 25)
                then "Domingo"  -- 000399.001368/2025-25 - Feriado de Natal
                when data = date(2025, 12, 26)
                then "Ponto Facultativo"  -- 000399.001368/2025-25 - Ponto Facultativo referente às festas de fim de ano
                when data = date(2025, 12, 29)
                then "Ponto Facultativo"  -- 000399.001368/2025-25 - Ponto Facultativo referente às festas de fim de ano
                when data = date(2025, 12, 30)
                then "Ponto Facultativo"  -- 000399.001368/2025-25 - Ponto Facultativo referente às festas de fim de ano
                when data = date(2025, 12, 31)
                then "Sabado"  -- 000399.001368/2025-25 - Operação Especial Réveillon
                when data = date(2026, 01, 01)
                then "Domingo"  -- 000399.001368/2025-25 - Operação Especial Réveillon
                when data = date(2026, 01, 02)
                then "Ponto Facultativo"  -- 000399.001368/2025-25 - Ponto Facultativo referente às festas de fim de ano
                when data = date(2026, 02, 13)
                then "Ponto Facultativo"  -- 000399.001590/2026-17 - Determinação de tipos dia de Carnaval
                when data between date(2026, 02, 16) and date(2026, 02, 17)
                then "Domingo"  -- 000399.001590/2026-17 - Determinação de tipos dia de Carnaval
                when data = date(2026, 02, 18)
                then "Sabado"  -- 000399.001590/2026-17 - Determinação de tipos dia de Carnaval
                when data = date(2026, 04, 02)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 57802 DE 30 DE MARÇO DE 2026
                when data = date(2026, 04, 03)
                then "Domingo"  -- Feriado sexta-feira da Paixão
                when data = date(2026, 04, 24)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 57867 DE 13 DE ABRIL DE 2026
                when data = date(2026, 06, 04)
                then "Domingo"  -- Feriado de Corpus Christi
                when data = date(2026, 06, 05)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 58105 DE 27 DE MAIO DE 2026
            end as tipo_dia,
            cast(null as string) as subtipo_dia,
            case
                when data between date(2024, 09, 14) and date(2024, 09, 15)
                then "Verão + Rock in Rio"
                when data between date(2024, 09, 19) and date(2024, 09, 22)
                then "Rock in Rio"
                when data = date(2024, 10, 06)
                then "Eleição"
                when data = date(2024, 11, 03)
                then "Enem"
                when data = date(2024, 11, 10)
                then "Enem"
                when data = date(2024, 11, 24)
                then "Parada LGBTQI+"  -- Processo.Rio MTR-DES-2024/70057
                when data between date(2024, 12, 07) and date(2024, 12, 08)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2024/72800
                when data between date(2024, 12, 14) and date(2024, 12, 15)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2024/74396
                when data = date(2024, 12, 23)
                then "Fim de ano"  -- Processo.Rio MTR-DES-2024/75723
                when data between date(2024, 12, 26) and date(2024, 12, 27)
                then "Fim de ano"  -- Processo.Rio MTR-DES-2024/75723
                when data = date(2024, 12, 30)
                then "Fim de ano"  -- Processo.Rio MTR-DES-2024/75723
                when data = date(2024, 12, 31)
                then "Vespera de Reveillon"  -- Processo.Rio MTR-DES-2024/76453
                when data = date(2025, 01, 01)
                then "Reveillon"  -- Processo.Rio MTR-DES-2024/76453
                when data between date(2025, 01, 02) and date(2025, 01, 03)
                then "Fim de ano"  -- Processo.Rio MTR-DES-2024/77046
                when data between date(2025, 01, 11) and date(2025, 01, 12)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2025/00831
                when data between date(2025, 01, 18) and date(2025, 01, 20)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2025/01760 e MTR-DES-2025/02195
                when data between date(2025, 01, 25) and date(2025, 01, 26)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2025/01468
                when data between date(2025, 02, 01) and date(2025, 02, 02)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2025/04515
                when data between date(2025, 02, 08) and date(2025, 02, 09)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/02376
                when data between date(2025, 02, 15) and date(2025, 02, 16)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/03046
                when data between date(2025, 02, 22) and date(2025, 02, 23)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/03740
                when data between date(2025, 03, 01) and date(2025, 03, 04)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/03920
                when data = date(2025, 03, 05)
                then "Atípico + Verão"  -- Processo.Rio MTR-PRO-2025/03920
                when data between date(2025, 03, 08) and date(2025, 03, 09)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/04520
                when data between date(2025, 05, 03) and date(2025, 05, 04)
                then "Dia Atípico"  -- Processo.Rio MTR-PRO-2025/04520
                when data = date(2025, 05, 24)
                then "Dia Atípico"
                when data between date(2025, 10, 04) and date(2025, 10, 05)
                then "Atípico"  -- MTR-DES-2025/89230 - Acionamento do Plano Verão
                when data = date(2025, 10, 12)
                then "Atípico"  -- MTR-DES-2025/89230 - Acionamento do Plano Verão (somente domingo)
                when data = date(2025, 11, 09)
                then "ENEM"  -- Operação especial em horário de prova do ENEM
                when data = date(2025, 11, 16)
                then "ENEM"  -- Operação especial em horário de prova do ENEM
                when data = date(2025, 11, 22)
                then "Verão"  -- MTR-DES-2025/110742 - Acionamento do Plano Verão
                when data = date(2025, 11, 23)
                then "Verão"  -- MTR-DES-2025/110742 - Acionamento do Plano Verão
                when data = date(2025, 11, 29)
                then "Verão"  -- MTR-DES-2025/116337 - Acionamento do Plano Verão
                when data = date(2025, 11, 30)
                then "Verão"  -- MTR-DES-2025/116337 - Acionamento do Plano Verão
                when data between date(2025, 12, 06) and date(2025, 12, 07)
                then "Verão"  -- 000399.000386/2025-90 - Acionamento do Plano Verão
                when data between date(2025, 12, 13) and date(2025, 12, 14)
                then "Verão"  -- 000399.000386/2025-90 - Acionamento do Plano Verão
                when data = date(2025, 12, 24)
                then "Natal"  -- 000399.001368/2025-25 - Véspera de Natal
                when data = date(2025, 12, 31)
                then "Reveillon_31-12"  -- 000399.001368/2025-25 - Operação Especial Réveillon
                when data = date(2026, 01, 01)
                then "Reveillon_01-01"  -- 000399.001368/2025-25 - Operação Especial Réveillon
                when data between date(2025, 12, 20) and date(2025, 12, 21)
                then "Verão"  -- 000399.001045/2025-31 - Acionamento do Plano Verão
                when data between date(2025, 12, 27) and date(2025, 12, 28)
                then "Verão"  -- 000399.001455/2025-82 - Acionamento do Plano Verão
                when data between date(2026, 01, 03) and date(2026, 01, 04)
                then "Verão"  -- 000399.001610/2025-61 - Acionamento do Plano Verão
                when data between date(2026, 01, 10) and date(2026, 01, 11)
                then "Verão"  -- 000399.000163/2026-11 - Acionamento do Plano Verão
                when data between date(2026, 01, 17) and date(2026, 01, 18)
                then "Verão"  -- 000399.000456/2026-91 - Acionamento do Plano Verão
                when data between date(2026, 01, 31) and date(2026, 02, 01)
                then "Verão"  -- 000399.001025/2026-41 - Acionamento do Plano Verão
                when data between date(2026, 02, 07) and date(2026, 02, 08)
                then "Verão"  -- 000399.001320/2026-06 - Acionamento do Plano Verão
                when data between date(2026, 02, 14) and date(2026, 02, 18)
                then "Verão"  -- 000399.001536/2026-63 - Acionamento do Plano Verão
                when data between date(2026, 02, 21) and date(2026, 02, 22)
                then "Verão"  -- 000399.001668/2026-95 - Acionamento do Plano Verão
                when data = date(2026, 03, 14)
                then "Term_Marg"  -- 000399.003413/2026-67 - Inauguração do Terminal Margaridas
                when data = date(2026, 05, 02)
                then "Shakira_02-05"  -- 000399.009326/2026-13 - Operação Especial para o dia 02/05/2026
                when data = date(2026, 05, 03)
                then "Shakira_03-05"  -- 000399.009326/2026-13 - Operação Especial para o dia 03/05/2026
                when data = date(2026, 05, 17)
                then "Oper_634"  -- 000399.011715/2026-17 - Inauguração 634
                when data = date(2026, 06, 13)
                then "Copa_13_Junho"  -- 000399.016030/2026-59 - Jogo da Copa
                when data = date(2026, 06, 19)
                then "Copa_19_Junho"  -- 000399.016030/2026-59 - Jogo da Copa
                when data = date(2026, 06, 24)
                then "Copa_24_Junho"  -- 000399.016030/2026-59 - Jogo da Copa
            end as tipo_os
        from
            unnest(
                generate_date_array(
                    {% if is_incremental() %}
                        date("{{ var('date_range_start') }}"),
                        date("{{ var('date_range_end') }}")
                    {% else %}date("2024-09-01"), current_date("America/Sao_Paulo")
                    {% endif %}
                )
            ) as data
        where data < date("{{ var('data_inicio_calendario_sheets') }}")
    ),
    datas as (
        select data, feed_start_date, tipo_dia, subtipo_dia, tipo_os
        from planilha

        union all

        select data, feed_start_date, tipo_dia, subtipo_dia, tipo_os
        from historico
    )
select *
from datas
where
    data is not null
    and (
        feed_start_date is not null
        or tipo_dia is not null
        or subtipo_dia is not null
        or tipo_os is not null
    )
