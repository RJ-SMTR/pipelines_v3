{{ config(materialized="ephemeral") }}

{% set gps_data_filter %}
    data between date('{{ var("date_range_start") }}') and date_add(
        date('{{ var("date_range_end") }}'), interval 1 day
    )
    and datetime_gps between datetime_trunc(
        date('{{ var("date_range_start") }}'), day
    ) and datetime_add(
        datetime_trunc(
            date_add(date('{{ var("date_range_end") }}'), interval 1 day), day
        ),
        interval 3 hour
    )
{% endset %}

with
    gps_onibus as (
        select
            data,
            datetime_gps,
            id_veiculo,
            trim(servico) as servico,
            latitude,
            longitude,
            fonte_gps,
            status,
            distancia
        from {{ ref("view_gps_onibus") }}
        where
            {{ gps_data_filter }}
            and status != "Parado garagem"
            and latitude is not null
            and longitude is not null
    ),
    gps_brt as (
        select
            data,
            datetime_gps,
            id_veiculo,
            trim(servico) as servico,
            latitude,
            longitude,
            "sonda" as fonte_gps,
            status,
            distancia
        from {{ ref("view_gps_brt_completo") }}
        where
            {{ gps_data_filter }}
            and ifnull(status, "") != "Parado garagem"
            and latitude is not null
            and longitude is not null
    ),
    gps_bordo as (
        select *
        from gps_onibus

        union all

        select *
        from gps_brt
    ),
    /*
    GPS de bordo SPPO usa prefixo (C47476); Jaé manda só a ordem (47476).
    Maxtrack (GTU/TUSA) usa A2-/B2-; hardcode das operadoras 2801/2802,
    mesmo padrão do gps_viagem.
    2801 - GTU (A2)
    2802 - TUSA (B2)

    */
    veiculos_bordo as (
        select substr(id_veiculo, 2) as id_veiculo_join, max(id_veiculo) as id_veiculo
        from gps_onibus
        group by id_veiculo_join
    ),
    gps_validador_filtrado as (
        select
            data,
            datetime_gps,
            coalesce(
                {{ id_veiculo_jae_rio("id_operadora", "id_veiculo") }}, id_veiculo
            ) as id_veiculo,
            trim(servico_jae) as servico,
            latitude,
            longitude,
            "jae" as fonte_gps,
            cast(null as string) as status,
            cast(null as float64) as distancia
        from {{ ref("gps_validador") }}
        where
            {{ gps_data_filter }}
            and id_veiculo is not null
            and id_veiculo != "99999"
            and modo = "Ônibus"
            and servico_jae is not null
            and latitude is not null
            and longitude is not null
            and latitude != 0
            and longitude != 0
    ),
    gps_validador as (
        select
            j.data,
            j.datetime_gps,
            coalesce(b.id_veiculo, j.id_veiculo) as id_veiculo,
            j.servico,
            j.latitude,
            j.longitude,
            j.fonte_gps,
            j.status,
            j.distancia
        from gps_validador_filtrado as j
        left join veiculos_bordo as b on j.id_veiculo = b.id_veiculo_join
    ),
    gps_combinado as (
        select *
        from gps_bordo

        union all

        select *
        from gps_validador
    ),
    /*
    Jaé só cobre lacunas do GPS de bordo: o ping da Jaé é descartado quando há
    ping de bordo do mesmo veículo a até N minutos (antes ou depois). Evita
    viagens paralelas quando bordo e Jaé informam serviço/shape diferentes.

    segundos_bordo_mais_proximo = menor distância (em segundos) entre o ping
    atual e o ping de bordo mais próximo do mesmo veículo, olhando para trás
    (último bordo antes) e para frente (primeiro bordo depois).

    if(fonte_gps != "jae", datetime_gps, null): coluna "virtual" com o horário
    só nas linhas de bordo (null nas da Jaé), para o ignore nulls achar o bordo.

    ifnull(..., 999999): sem bordo de um dos lados (primeiro/último ping do
    veículo é da Jaé) o lado vale "infinito" e não conta como perto. Veículo
    sem nenhum bordo na janela fica 999999 dos dois lados: Jaé é mantida.

    Exemplo (veículo C47476, janela de 2 min = 120 s):

    datetime_gps | fonte | bordo antes | bordo depois | mais próximo | resultado
    -------------+-------+-------------+--------------+--------------+--------------------------
    10:00:00     | zirix |      -      |      -       |      -       | fica (bordo)
    10:00:20     | jae   | 10:00:00    | 10:01:00     |     20 s     | sai  (bordo transmitindo)
    10:01:00     | zirix |      -      |      -       |      -       | fica (bordo)
    10:03:40     | jae   | 10:01:00    | 10:07:00     |    160 s     | fica (lacuna do bordo)
    10:06:00     | jae   | 10:01:00    | 10:07:00     |     60 s     | sai  (bordo volta 10:07)
    10:07:00     | zirix |      -      |      -       |      -       | fica (bordo)
    */
    gps_lacuna_bordo as (
        select
            *,
            -- menor das duas distâncias: bordo antes x bordo depois
            least(
                -- segundos desde o último ping de bordo (olhando para trás)
                ifnull(
                    datetime_diff(
                        datetime_gps,
                        last_value(
                            if(fonte_gps != "jae", datetime_gps, null) ignore nulls
                        ) over (
                            -- cada veículo é analisado separadamente
                            partition by id_veiculo
                            order by datetime_gps
                            -- do primeiro ping do veículo até a linha atual
                            rows between unbounded preceding and current row
                        ),
                        second
                    ),
                    999999
                ),
                -- segundos até o próximo ping de bordo (olhando para frente)
                ifnull(
                    datetime_diff(
                        first_value(
                            if(fonte_gps != "jae", datetime_gps, null) ignore nulls
                        ) over (
                            partition by id_veiculo
                            order by datetime_gps
                            -- da linha atual até o último ping do veículo
                            rows between current row and unbounded following
                        ),
                        datetime_gps,
                        second
                    ),
                    999999
                )
            ) as segundos_bordo_mais_proximo
        from gps_combinado
    )
-- Bordo sempre fica; Jaé só fica se o bordo mais próximo estiver a mais de N
-- minutos (var janela_jae_bordo_minutos, padrão 2 = 120 s)
select * except (segundos_bordo_mais_proximo)
from gps_lacuna_bordo
where
    fonte_gps != "jae"
    or segundos_bordo_mais_proximo > {{ var("janela_jae_bordo_minutos") }} * 60
