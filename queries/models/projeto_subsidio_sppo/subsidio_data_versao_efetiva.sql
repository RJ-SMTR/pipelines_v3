{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% if var("run_date") <= var("DATA_SUBSIDIO_V6_INICIO") %}
    {% if execute %}
        {% set trips_date = (
            run_query(
                "SELECT MAX(data_versao) FROM "
                ~ ref("subsidio_trips_desaninhada")
                ~ " WHERE data_versao >= DATE_TRUNC(DATE_SUB(DATE('"
                ~ var("run_date")
                ~ "'), INTERVAL 30 DAY), MONTH)"
            )
            .columns[0]
            .values()[0]
        ) %}
        {% set shapes_date = (
            run_query(
                "SELECT MAX(data_versao) FROM "
                ~ var("subsidio_shapes")
                ~ " WHERE data_versao >= DATE_TRUNC(DATE_SUB(DATE('"
                ~ var("run_date")
                ~ "'), INTERVAL 30 DAY), MONTH)"
            )
            .columns[0]
            .values()[0]
        ) %}
        {% set frequencies_date = (
            run_query(
                "SELECT MAX(data_versao) FROM "
                ~ ref("subsidio_quadro_horario")
                ~ " WHERE data_versao >= DATE_TRUNC(DATE_SUB(DATE('"
                ~ var("run_date")
                ~ "'), INTERVAL 30 DAY), MONTH)"
            )
            .columns[0]
            .values()[0]
        ) %}
    {% endif %}

    with
        dates as (
            select
                data,
                case
                    when data = "2022-06-16"
                    then "Domingo"
                    when data = "2022-06-17"
                    then "Sabado"
                    when data = "2022-09-02"
                    then "Sabado"
                    when data = "2022-09-07"
                    then "Domingo"
                    when data = "2022-10-12"
                    then "Domingo"
                    when data = "2022-10-17"
                    then "Sabado"
                    when data = "2022-11-02"
                    then "Domingo"
                    when data = "2022-11-14"
                    then "Sabado"
                    when data = "2022-11-15"
                    then "Domingo"
                    when data = "2022-11-24"
                    then "Sabado"
                    when data = "2022-11-28"
                    then "Sabado"
                    when data = "2022-12-02"
                    then "Sabado"
                    when data = "2022-12-05"
                    then "Sabado"
                    when data = "2022-12-09"
                    then "Sabado"
                    when data = "2023-04-06"
                    then "Sabado"  -- Ponto Facultativo - DECRETO RIO Nº 52275/2023
                    when data = "2023-04-07"
                    then "Domingo"  -- Paixão de Cristo -- Art. 1º, V - PORTARIA ME Nº 11.090/2022
                    when data = "2023-06-08"
                    then "Domingo"  -- Corpus Christi - Lei nº 336/1949 - OFÍCIO Nº MTR-OFI-2023/03260 (MTROFI202303260A)
                    when data = "2023-06-09"
                    then "Sabado"  -- Ponto Facultativo - DECRETO RIO Nº 52584/2023
                    when data = "2023-09-08"
                    then "Ponto Facultativo"  -- Ponto Facultativo - DECRETO RIO Nº 53137/2023
                    when data = "2023-10-13"
                    then "Ponto Facultativo"  -- Ponto Facultativo - DECRETO RIO Nº 53296/2023
                    when data = "2023-10-16"
                    then "Ponto Facultativo"  -- Dia do Comércio - OS Outubro/Q2
                    when data = "2023-11-03"
                    then "Ponto Facultativo"  -- Ponto Facultativo - DECRETO RIO Nº 53417/2023
                    when data = "2023-11-05"
                    then "Sabado"  -- Domingo Atípico - ENEM - OS Novembro/Q1
                    when data = "2023-11-12"
                    then "Sabado"  -- Domingo Atípico - ENEM - OS Novembro/Q1
                    when data = "2023-12-02"
                    then "Sabado - Verão"  -- OS de Verão
                    when data = "2023-12-03"
                    then "Domingo - Verão"  -- OS de Verão
                    when data = "2023-12-16"
                    then "Sabado - Verão"  -- OS de Verão
                    when data = "2023-12-17"
                    then "Domingo - Verão"  -- OS de Verão
                    when data = "2024-01-06"
                    then "Sabado - Verão"  -- OS de Verão
                    when data = "2024-01-07"
                    then "Domingo - Verão"  -- OS de Verão
                    when data = "2024-02-09"
                    then "Ponto Facultativo"  -- Despacho MTR-DES-2024/07951
                    when data = "2024-02-12"
                    then "Domingo"  -- Despacho MTR-DES-2024/07951
                    when data = "2024-02-13"
                    then "Domingo"  -- Despacho MTR-DES-2024/07951
                    when data = "2024-02-14"
                    then "Ponto Facultativo"  -- Despacho MTR-DES-2024/07951
                    when data = "2023-12-31"
                    then "Domingo - Réveillon"
                    when data = "2024-01-01"
                    then "Domingo - Réveillon"
                    when data = "2024-02-24"
                    then "Sabado - Verão"  -- OS de Verão - Despacho MTR-DES-2024/10516
                    when data = "2024-02-25"
                    then "Domingo - Verão"  -- OS de Verão - Despacho MTR-DES-2024/10516
                    when data = "2024-03-16"
                    then "Sabado - Verão"  -- OS de Verão - Despacho MTR-DES-2024/15504
                    when data = "2024-03-17"
                    then "Domingo - Verão"  -- OS de Verão - Despacho MTR-DES-2024/15504
                    when data = "2024-03-22"
                    then "Ponto Facultativo"  -- Ponto Facultativo - DECRETO RIO Nº 54114/2024
                    when data = "2024-03-28"
                    then "Ponto Facultativo"  -- Ponto Facultativo - DECRETO RIO Nº 54081/2024
                    when data = "2024-03-29"
                    then "Domingo"  -- Feriado de Paixão de Cristo (Sexta-feira Santa)
                    when extract(day from data) = 20 and extract(month from data) = 1
                    then "Domingo"  -- Dia de São Sebastião -- Art. 8°, I - Lei Municipal nº 5146/2010
                    when extract(day from data) = 23 and extract(month from data) = 4
                    then "Domingo"  -- Dia de São Jorge -- Art. 8°, II - Lei Municipal nº 5146/2010 / Lei Estadual Nº 5198/2008 / Lei Estadual Nº 5645/2010
                    when extract(day from data) = 20 and extract(month from data) = 11
                    then "Domingo"  -- Aniversário de morte de Zumbi dos Palmares / Dia da Consciência Negra -- Art. 8°, IV - Lei Municipal nº 5146/2010 / Lei Estadual nº 526/1982 / Lei Estadual nº 1929/1991 / Lei Estadual nº 4007/2002 / Lei Estadual Nº 5645/2010
                    when extract(day from data) = 21 and extract(month from data) = 4
                    then "Domingo"  -- Tiradentes -- Art. 1º, VI - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 1 and extract(month from data) = 5
                    then "Domingo"  -- Dia Mundial do Trabalho -- Art. 1º, VII - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 7 and extract(month from data) = 9
                    then "Domingo"  -- Independência do Brasil -- Art. 1º, IX - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 12 and extract(month from data) = 10
                    then "Domingo"  -- Nossa Senhora Aparecida -- Art. 1º, X - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 2 and extract(month from data) = 11
                    then "Domingo"  -- Finados -- Art. 1º, XII - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 15 and extract(month from data) = 11
                    then "Domingo"  -- Proclamação da República -- Art. 1º, XIII - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 25 and extract(month from data) = 12
                    then "Domingo"  -- Natal -- Art. 1º, XIV - PORTARIA ME Nº 11.090/2022
                    when extract(dayofweek from data) = 1
                    then "Domingo"
                    when extract(dayofweek from data) = 7
                    then "Sabado"
                    else "Dia Útil"
                end as tipo_dia,
                case
                    -- Reveillon 2022:
                    when data = date(2022, 12, 31)
                    then data
                    when data = date(2023, 1, 1)
                    then data
                    when data between date(2023, 1, 2) and date(2023, 1, 15)
                    then date(2023, 1, 2)
                    -- Reprocessamento:
                    when data between date(2023, 1, 15) and date(2023, 1, 31)
                    then date(2023, 1, 16)
                    when data between date(2023, 3, 16) and date(2023, 3, 31)
                    then date(2023, 3, 16)
                    -- Alteração de Planejamento
                    when data between date(2023, 6, 16) and date(2023, 6, 30)
                    then date(2023, 6, 16)
                    when data between date(2023, 7, 16) and date(2023, 7, 31)
                    then date(2023, 7, 16)
                    when data between date(2023, 8, 16) and date(2023, 8, 31)
                    then date(2023, 8, 16)
                    when data between date(2023, 9, 16) and date(2023, 9, 30)
                    then date(2023, 9, 16)
                    when data between date(2023, 10, 16) and date(2023, 10, 16)
                    then date(2023, 10, 16)
                    when data between date(2023, 10, 17) and date(2023, 10, 23)
                    then date(2023, 10, 17)
                    when data between date(2023, 10, 24) and date(2023, 10, 31)
                    then date(2023, 10, 24)
                    when data = date(2023, 12, 01)
                    then data  -- Desvio do TIG
                    when data between date(2023, 12, 02) and date(2023, 12, 03)
                    then date(2023, 12, 03)  -- OS de Verão
                    when data between date(2023, 12, 16) and date(2023, 12, 17)
                    then date(2023, 12, 03)  -- OS de Verão
                    when data between date(2023, 12, 04) and date(2023, 12, 20)
                    then date(2023, 12, 02)  -- Fim do desvio do TIG
                    when data between date(2023, 12, 21) and date(2023, 12, 30)
                    then date(2023, 12, 21)
                    -- Reveillon 2023:
                    when data = date(2023, 12, 31)
                    then data
                    when data = date(2024, 01, 01)
                    then data
                    -- 2024:
                    when data between date(2024, 01, 06) and date(2024, 01, 07)
                    then date(2024, 01, 03)  -- OS de Verão
                    when data between date(2024, 01, 02) and date(2024, 01, 14)
                    then date(2024, 01, 02)
                    when data between date(2024, 01, 15) and date(2024, 01, 31)
                    then date(2024, 01, 15)
                    when data between date(2024, 02, 01) and date(2024, 02, 18)
                    then date(2024, 02, 01)  -- OS fev/Q1
                    when data between date(2024, 02, 19) and date(2024, 02, 23)
                    then date(2024, 02, 19)  -- OS fev/Q2
                    when data between date(2024, 02, 24) and date(2024, 02, 25)
                    then date(2024, 02, 25)  -- OS fev/Q2 - TIG - OS Verão
                    when data between date(2024, 02, 26) and date(2024, 03, 01)
                    then date(2024, 02, 24)  -- OS fev/Q2 - TIG
                    when data between date(2024, 03, 02) and date(2024, 03, 10)
                    then date(2024, 03, 02)  -- OS mar/Q1
                    when data between date(2024, 03, 11) and date(2024, 03, 15)
                    then date(2024, 03, 11)  -- OS mar/Q1
                    when data between date(2024, 03, 16) and date(2024, 03, 17)
                    then date(2024, 03, 12)  -- OS mar/Q2
                    when data between date(2024, 03, 18) and date(2024, 03, 29)
                    then date(2024, 03, 18)  -- OS mar/Q2
                    when data between date(2024, 03, 30) and date(2024, 04, 30)
                    then date(2024, 03, 30)  -- OS abr/Q1
                    -- 2022:
                    when data between date(2022, 10, 1) and date(2022, 10, 2)
                    then date(2022, 9, 16)
                    when
                        data
                        between date(2022, 6, 1) and last_day(date(2022, 6, 30), month)
                    then date(2022, 6, 1)
                    {% for i in range(7, 13) %}
                        when
                            data
                            between date(2022,{{ i }}, 1) and date(2022,{{ i }}, 15)
                        then date(2022,{{ i }}, 1)
                        when
                            data between date(2022,{{ i }}, 16) and last_day(
                                date(2022,{{ i }}, 30), month
                            )
                        then date(2022,{{ i }}, 16)
                    {% endfor %}
                    -- 2023 a 2024:
                    {% for j in range(2023, 2025) %}
                        {% for i in range(1, 13) %}
                            when
                                extract(month from data) = {{ i }}
                                and extract(year from data) = {{ j }}
                            then date({{ j }},{{ i }}, 1)
                        {% endfor %}
                    {% endfor %}
                end as data_versao_trips,
                case
                    -- Reveillon 2022:
                    when data = date(2022, 12, 31)
                    then data
                    when data = date(2023, 1, 1)
                    then data
                    when data between date(2023, 1, 2) and date(2023, 1, 15)
                    then date(2023, 1, 2)
                    -- Reprocessamento:
                    when data between date(2023, 1, 15) and date(2023, 1, 31)
                    then date(2023, 1, 16)
                    when data between date(2023, 3, 16) and date(2023, 3, 31)
                    then date(2023, 3, 16)
                    -- Alteração de Planejamento
                    when data between date(2023, 6, 16) and date(2023, 6, 30)
                    then date(2023, 6, 16)
                    when data between date(2023, 7, 16) and date(2023, 7, 31)
                    then date(2023, 7, 16)
                    when data between date(2023, 8, 16) and date(2023, 8, 31)
                    then date(2023, 8, 16)
                    when data between date(2023, 9, 16) and date(2023, 9, 30)
                    then date(2023, 9, 16)
                    when data between date(2023, 10, 16) and date(2023, 10, 16)
                    then date(2023, 10, 16)
                    when data between date(2023, 10, 17) and date(2023, 10, 23)
                    then date(2023, 10, 17)
                    when data between date(2023, 10, 24) and date(2023, 10, 31)
                    then date(2023, 10, 24)
                    when data = date(2023, 12, 01)
                    then data  -- Desvio do TIG
                    when data between date(2023, 12, 02) and date(2023, 12, 03)
                    then date(2023, 12, 03)  -- OS de Verão
                    when data between date(2023, 12, 16) and date(2023, 12, 17)
                    then date(2023, 12, 03)  -- OS de Verão
                    when data between date(2023, 12, 04) and date(2023, 12, 20)
                    then date(2023, 12, 02)  -- Fim do desvio do TIG
                    when data between date(2023, 12, 21) and date(2023, 12, 30)
                    then date(2023, 12, 21)
                    -- Reveillon 2023:
                    when data = date(2023, 12, 31)
                    then data
                    when data = date(2024, 01, 01)
                    then data
                    -- 2024:
                    when data between date(2024, 01, 06) and date(2024, 01, 07)
                    then date(2024, 01, 03)  -- OS de Verão
                    when data between date(2024, 01, 02) and date(2024, 01, 14)
                    then date(2024, 01, 02)
                    when data between date(2024, 01, 15) and date(2024, 01, 31)
                    then date(2024, 01, 15)
                    when data between date(2024, 02, 01) and date(2024, 02, 18)
                    then date(2024, 02, 01)  -- OS fev/Q1
                    when data between date(2024, 02, 19) and date(2024, 02, 23)
                    then date(2024, 02, 19)  -- OS fev/Q2
                    when data between date(2024, 02, 24) and date(2024, 02, 25)
                    then date(2024, 02, 25)  -- OS fev/Q2 - TIG - OS Verão
                    when data between date(2024, 02, 26) and date(2024, 03, 01)
                    then date(2024, 02, 24)  -- OS fev/Q2 - TIG
                    when data between date(2024, 03, 02) and date(2024, 03, 10)
                    then date(2024, 03, 02)  -- OS mar/Q1
                    when data between date(2024, 03, 11) and date(2024, 03, 15)
                    then date(2024, 03, 11)  -- OS mar/Q1
                    when data between date(2024, 03, 16) and date(2024, 03, 17)
                    then date(2024, 03, 12)  -- OS mar/Q2
                    when data between date(2024, 03, 18) and date(2024, 03, 29)
                    then date(2024, 03, 18)  -- OS mar/Q2
                    when data between date(2024, 03, 30) and date(2024, 04, 30)
                    then date(2024, 03, 30)  -- OS abr/Q1
                    -- 2022:
                    when data between date(2022, 10, 1) and date(2022, 10, 2)
                    then date(2022, 9, 16)
                    when
                        data
                        between date(2022, 6, 1) and last_day(date(2022, 6, 30), month)
                    then date(2022, 6, 1)
                    {% for i in range(7, 13) %}
                        when
                            data
                            between date(2022,{{ i }}, 1) and date(2022,{{ i }}, 15)
                        then date(2022,{{ i }}, 1)
                        when
                            data between date(2022,{{ i }}, 16) and last_day(
                                date(2022,{{ i }}, 30), month
                            )
                        then date(2022,{{ i }}, 16)
                    {% endfor %}
                    -- 2023 a 2024:
                    {% for j in range(2023, 2025) %}
                        {% for i in range(1, 13) %}
                            when
                                extract(month from data) = {{ i }}
                                and extract(year from data) = {{ j }}
                            then date({{ j }},{{ i }}, 1)
                        {% endfor %}
                    {% endfor %}
                end as data_versao_shapes,
                case
                    -- Reveillon 2022:
                    when data = date(2022, 12, 31)
                    then data
                    when data = date(2023, 1, 1)
                    then data
                    when data between date(2023, 1, 2) and date(2023, 1, 15)
                    then date(2023, 1, 2)
                    -- Reprocessamento:
                    when data between date(2023, 1, 15) and date(2023, 1, 31)
                    then date(2023, 1, 16)
                    when data between date(2023, 3, 16) and date(2023, 3, 31)
                    then date(2023, 3, 16)
                    -- Alteração de Planejamento
                    when data between date(2023, 6, 16) and date(2023, 6, 30)
                    then date(2023, 6, 16)
                    when data between date(2023, 7, 16) and date(2023, 7, 31)
                    then date(2023, 7, 16)
                    when data between date(2023, 8, 16) and date(2023, 8, 31)
                    then date(2023, 8, 16)
                    when data between date(2023, 9, 16) and date(2023, 9, 30)
                    then date(2023, 9, 16)
                    when data between date(2023, 10, 16) and date(2023, 10, 16)
                    then date(2023, 10, 16)
                    when data between date(2023, 10, 17) and date(2023, 10, 23)
                    then date(2023, 10, 17)
                    when data between date(2023, 10, 24) and date(2023, 10, 31)
                    then date(2023, 10, 24)
                    when data = date(2023, 12, 01)
                    then data  -- Desvio do TIG
                    when data between date(2023, 12, 02) and date(2023, 12, 03)
                    then date(2023, 12, 03)  -- OS de Verão
                    when data between date(2023, 12, 16) and date(2023, 12, 17)
                    then date(2023, 12, 03)  -- OS de Verão
                    when data between date(2023, 12, 04) and date(2023, 12, 20)
                    then date(2023, 12, 02)  -- Fim do desvio do TIG
                    when data between date(2023, 12, 21) and date(2023, 12, 30)
                    then date(2023, 12, 21)
                    -- Reveillon 2023:
                    when data = date(2023, 12, 31)
                    then data
                    when data = date(2024, 01, 01)
                    then data
                    -- 2024:
                    when data between date(2024, 01, 06) and date(2024, 01, 07)
                    then date(2024, 01, 03)  -- OS de Verão
                    when data between date(2024, 01, 02) and date(2024, 01, 14)
                    then date(2024, 01, 02)
                    when data between date(2024, 01, 15) and date(2024, 01, 31)
                    then date(2024, 01, 15)
                    when data between date(2024, 02, 01) and date(2024, 02, 18)
                    then date(2024, 02, 01)  -- OS fev/Q1
                    when data between date(2024, 02, 19) and date(2024, 02, 23)
                    then date(2024, 02, 19)  -- OS fev/Q2
                    when data between date(2024, 02, 24) and date(2024, 02, 25)
                    then date(2024, 02, 25)  -- OS fev/Q2 - TIG - OS Verão
                    when data between date(2024, 02, 26) and date(2024, 03, 01)
                    then date(2024, 02, 24)  -- OS fev/Q2 - TIG
                    when data between date(2024, 03, 02) and date(2024, 03, 10)
                    then date(2024, 03, 02)  -- OS mar/Q1
                    when data between date(2024, 03, 11) and date(2024, 03, 15)
                    then date(2024, 03, 11)  -- OS mar/Q1
                    when data between date(2024, 03, 16) and date(2024, 03, 17)
                    then date(2024, 03, 12)  -- OS mar/Q2
                    when data between date(2024, 03, 18) and date(2024, 03, 29)
                    then date(2024, 03, 18)  -- OS mar/Q2
                    when data between date(2024, 03, 30) and date(2024, 04, 30)
                    then date(2024, 03, 30)  -- OS abr/Q1
                    -- 2022:
                    {% for i in range(6, 13) %}
                        when
                            data
                            between date(2022,{{ i }}, 1) and date(2022,{{ i }}, 15)
                        then date(2022,{{ i }}, 1)
                        when
                            data between date(2022,{{ i }}, 16) and last_day(
                                date(2022,{{ i }}, 30), month
                            )
                        then date(2022,{{ i }}, 16)
                    {% endfor %}
                    -- 2023 a 2024:
                    {% for j in range(2023, 2025) %}
                        {% for i in range(1, 13) %}
                            when
                                extract(month from data) = {{ i }}
                                and extract(year from data) = {{ j }}
                            then date({{ j }},{{ i }}, 1)
                        {% endfor %}
                    {% endfor %}
                end as data_versao_frequencies,
                case
                    when extract(year from data) = 2022
                    then
                        (
                            case
                                when extract(month from data) = 6
                                then 2.13
                                when extract(month from data) = 7
                                then 1.84
                                when extract(month from data) = 8
                                then 1.80
                                when extract(month from data) = 9
                                then 1.75
                                when extract(month from data) = 10
                                then 1.62
                                when extract(month from data) = 11
                                then 1.53
                                when extract(month from data) = 12
                                then 1.78
                            end
                        )
                    when extract(year from data) = 2023
                    then (case when data <= date("2023-01-06") then 3.18 else 2.81 end)
                end as valor_subsidio_por_km
            from
                unnest(
                    generate_date_array(
                        "2022-06-01",
                        date_sub("{{var('DATA_SUBSIDIO_V6_INICIO')}}", interval 1 day)
                    )
                ) as data
        ),
        trips as (
            select distinct data_versao
            from {{ ref("subsidio_trips_desaninhada") }}
            {% if is_incremental() %}
                where
                    data_versao >= date_trunc(
                        date_sub(date("{{ var('run_date') }}"), interval 30 day), month
                    )
            {% endif %}
        ),
        shapes as (
            select distinct data_versao
            from {{ var("subsidio_shapes") }}
            {% if is_incremental() %}
                where
                    data_versao >= date_trunc(
                        date_sub(date("{{ var('run_date') }}"), interval 30 day), month
                    )
            {% endif %}
        ),
        frequencies as (
            select distinct data_versao
            from {{ ref("subsidio_quadro_horario") }}
            {% if is_incremental() %}
                where
                    data_versao >= date_trunc(
                        date_sub(date("{{ var('run_date') }}"), interval 30 day), month
                    )
            {% endif %}
        )
    select
        data,
        tipo_dia,
        safe_cast(null as string) as subtipo_dia,
        coalesce(t.data_versao, date("{{ trips_date }}")) as data_versao_trips,
        coalesce(s.data_versao, date("{{ shapes_date }}")) as data_versao_shapes,
        coalesce(
            f.data_versao, date("{{ frequencies_date }}")
        ) as data_versao_frequencies,
        valor_subsidio_por_km,
        safe_cast(null as string) as feed_version,
        safe_cast(null as date) as feed_start_date,
        safe_cast(null as string) as tipo_os,
    from dates as d
    left join trips as t on t.data_versao = d.data_versao_trips
    left join shapes as s on s.data_versao = d.data_versao_shapes
    left join frequencies as f on f.data_versao = d.data_versao_frequencies
    where
        {% if is_incremental() %}
            data
            between date_sub(date("{{ var('run_date') }}"), interval 1 day) and date(
                "{{ var('run_date') }}"
            )
        {% else %} data <= date("{{ var('run_date') }}")
        {% endif %}

{% else %}

    with
        dates as (
            select
                data,
                case
                    when data = "2024-04-22"
                    then "Ponto Facultativo"  -- Ponto Facultativo - DECRETO RIO Nº 54267/2024
                    when data = "2024-05-30"
                    then "Domingo"  -- Feriado de Corpus Christi - (Decreto Rio Nº 54525/2024)
                    when data = "2024-05-31"
                    then "Ponto Facultativo"  -- Ponto Facultativo - (Decreto Rio Nº 54525/2024)
                    when data = "2024-10-21"
                    then "Ponto Facultativo"  -- Ponto Facultativo - Dia do Comérciario - (Processo.Rio MTR-DES-2024/64171)
                    when data = "2024-10-28"
                    then "Ponto Facultativo"  -- Ponto Facultativo - Dia do Servidor Público - (Processo.Rio MTR-DES-2024/64417)
                    when data between date(2024, 11, 18) and date(2024, 11, 19)
                    then "Ponto Facultativo"  -- Ponto Facultativo - G20 - (Processo.Rio MTR-DES-2024/67477)
                    when data = date(2024, 12, 24)
                    then "Ponto Facultativo"  -- Ponto Facultativo - Véspera de Natal - (Processo.Rio MTR-DES-2024/75723)
                    when data = date(2025, 02, 28)
                    then "Ponto Facultativo"  -- Ponto Facultativo - Sexta-feira de Carnaval - (Processo.Rio MTR-PRO-2025/03920)
                    when extract(day from data) = 20 and extract(month from data) = 1
                    then "Domingo"  -- Dia de São Sebastião -- Art. 8°, I - Lei Municipal nº 5146/2010
                    when extract(day from data) = 23 and extract(month from data) = 4
                    then "Domingo"  -- Dia de São Jorge -- Art. 8°, II - Lei Municipal nº 5146/2010 / Lei Estadual Nº 5198/2008 / Lei Estadual Nº 5645/2010
                    when extract(day from data) = 20 and extract(month from data) = 11
                    then "Domingo"  -- Aniversário de morte de Zumbi dos Palmares / Dia da Consciência Negra -- Art. 8°, IV - Lei Municipal nº 5146/2010 / Lei Estadual nº 526/1982 / Lei Estadual nº 1929/1991 / Lei Estadual nº 4007/2002 / Lei Estadual Nº 5645/2010
                    when extract(day from data) = 21 and extract(month from data) = 4
                    then "Domingo"  -- Tiradentes -- Art. 1º, VI - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 1 and extract(month from data) = 5
                    then "Domingo"  -- Dia Mundial do Trabalho -- Art. 1º, VII - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 7 and extract(month from data) = 9
                    then "Domingo"  -- Independência do Brasil -- Art. 1º, IX - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 12 and extract(month from data) = 10
                    then "Domingo"  -- Nossa Senhora Aparecida -- Art. 1º, X - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 2 and extract(month from data) = 11
                    then "Domingo"  -- Finados -- Art. 1º, XII - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 15 and extract(month from data) = 11
                    then "Domingo"  -- Proclamação da República -- Art. 1º, XIII - PORTARIA ME Nº 11.090/2022
                    when extract(day from data) = 25 and extract(month from data) = 12
                    then "Domingo"  -- Natal -- Art. 1º, XIV - PORTARIA ME Nº 11.090/2022
                    when extract(dayofweek from data) = 1
                    then "Domingo"
                    when extract(dayofweek from data) = 7
                    then "Sabado"
                    else "Dia Útil"
                end as tipo_dia,
                case
                    when data between date(2024, 03, 11) and date(2024, 03, 17)
                    then "2024-03-11"  -- OS mar/Q1
                    when data between date(2024, 03, 18) and date(2024, 03, 29)
                    then "2024-03-18"  -- OS mar/Q2
                    when data between date(2024, 03, 30) and date(2024, 04, 14)
                    then "2024-03-30"  -- OS abr/Q1
                    when data between date(2024, 04, 15) and date(2024, 05, 02)
                    then "2024-04-15"  -- OS abr/Q2
                    when data between date(2024, 05, 03) and date(2024, 05, 14)
                    then "2024-05-03"  -- OS maio/Q1
                end as feed_version,
                case
                    when data = date(2024, 05, 04)
                    then "Madonna 2024-05-04"
                    when data = date(2024, 05, 05)
                    then "Madonna 2024-05-05"
                    when data = date(2024, 08, 18)
                    then "CNU"  -- Processo.Rio MTR-PRO-2024/13252
                    when data = date(2024, 09, 13)
                    then "Rock in Rio"
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
                    else "Regular"
                end as tipo_os,
            from
                unnest(
                    generate_date_array(
                        "{{var('DATA_SUBSIDIO_V6_INICIO')}}", "{{ var('run_date') }}"
                    )
                ) as data
        ),
        data_versao_efetiva_manual as (
            select
                data,
                ifnull(c.tipo_dia, d.tipo_dia) as tipo_dia,
                case
                    when ifnull(c.tipo_os, d.tipo_os) = "Extraordinária - Verão"
                    then "Verão"
                    when ifnull(c.tipo_os, d.tipo_os) like "%Madonna%"
                    then "Madonna"
                    when ifnull(c.tipo_os, d.tipo_os) like "%Reveillon%"
                    then "Reveillon"
                    when
                        data between date(2025, 05, 03) and date(2025, 05, 04)
                        and ifnull(c.tipo_os, d.tipo_os) = "Dia Atípico"
                    then "Lady Gaga"  -- Processo.Rio MTR-PRO-2025/04520 [Operação Especial "Todo Mundo no Rio" - Lady Gaga]
                    when
                        data = date(2025, 05, 24)
                        and ifnull(c.tipo_os, d.tipo_os) = "Dia Atípico"
                    then "Marcha para Jesus"  -- [processo] [Operação especial evento "Marcha para Jesus"]
                    when
                        data = date(2025, 12, 24)
                        and ifnull(c.tipo_os, d.tipo_os) = "Natal"
                    then "Véspera de Natal"  -- 000399.001368/2025-25 - Véspera de Natal
                    when
                        data between date(2025, 12, 31) and date(2026, 01, 01)
                        and ifnull(c.tipo_os, d.tipo_os)
                        in ("Reveillon_31-12", "Reveillon_01-01")
                    then "Ano Novo"  -- 000399.001368/2025-25 - Operação Especial Réveillon
                    when ifnull(c.tipo_os, d.tipo_os) = "Regular"
                    then null
                    else ifnull(c.tipo_os, d.tipo_os)
                end as subtipo_dia,
                i.feed_version,
                i.feed_start_date,
                ifnull(c.tipo_os, d.tipo_os) as tipo_os,
            from dates as d
            left join {{ ref("feed_info_gtfs") }} as i using (feed_version)
            {# `rj-smtr.gtfs.feed_info` as i using (feed_version) #}
            left join {{ ref("aux_calendario_manual") }} as c using (data)
            where
                {% if is_incremental() %}
                    data = date_sub(date("{{ var('run_date') }}"), interval 1 day)
                {% else %} data <= date("{{ var('run_date') }}")
                {% endif %}
        )
    select
        data,
        tipo_dia,
        subtipo_dia,
        safe_cast(null as date) as data_versao_trips,
        safe_cast(null as date) as data_versao_shapes,
        safe_cast(null as date) as data_versao_frequencies,
        safe_cast(null as float64) as valor_subsidio_por_km,
        coalesce(d.feed_version, i.feed_version) as feed_version,
        coalesce(d.feed_start_date, i.feed_start_date) as feed_start_date,
        tipo_os,
    from data_versao_efetiva_manual as d
    left join
        {{ ref("feed_info_gtfs") }} as i
        {# `rj-smtr.gtfs.feed_info` as i #}
        on (
            data between i.feed_start_date and i.feed_end_date
            or (data >= i.feed_start_date and i.feed_end_date is null)
        )

{% endif %}
