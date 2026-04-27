{{ config(materialized="ephemeral") }}

/*
Descrição:
Calcula se o veículo está dentro do trajeto correto dado o traçado (shape) cadastrado no SIGMOB em relação à linha que está sendo
transmitida.
1. Calcula as intersecções definindo um 'buffer', utilizado por st_dwithin para identificar se o ponto está à uma
distância menor ou igual ao tamanho do buffer em relação ao traçado definido no SIGMOB.
2. Calcula um histórico de intersecções nos ultimos 10 minutos de registros de cada carro. Definimos que o carro é
considerado fora do trajeto definido se a cada 10 minutos, ele não esteve dentro do traçado planejado pelo menos uma
vez.
3. Identifica se a linha informada no registro capturado existe nas definições presentes no SIGMOB.
4. Definimos em outra tabela uma 'data_versao_efetiva', esse passo serve tanto para definir qual versão do SIGMOB utilizaremos em
caso de falha na captura, quanto para definir qual versão será utilizada para o cálculo retroativo do histórico de registros que temos.
5. Como não conseguimos identificar o itinerário que o carro está realizando, no passo counts, os resultados de
intersecções são dobrados, devido ao fato de cada linha apresentar dois itinerários possíveis (ida/volta). Portanto,
ao final, realizamos uma agregação LOGICAL_OR que é true caso o carro esteja dentro do traçado de algum dos itinerários
possíveis para a linha informada.
*/
with
    registros as (
        select
            id_veiculo,
            linha,
            latitude,
            longitude,
            data,
            posicao_veiculo_geo,
            timestamp_gps
        from {{ ref("sppo_aux_registros_filtrada") }} r
        {% if not flags.FULL_REFRESH -%}
            where
                data between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and timestamp_gps > "{{var('date_range_start')}}"
                and timestamp_gps <= "{{var('date_range_end')}}"
        {%- endif -%}
    ),
    intersec as (
        select
            r.*,
            s.data_versao,
            s.linha_gtfs,
            s.route_id,
            -- 1. Buffer e intersecções
            case
                when
                    st_dwithin(
                        shape, posicao_veiculo_geo, {{ var("tamanho_buffer_metros") }}
                    )
                then true
                else false
            end as flag_trajeto_correto,
            -- 2. Histórico de intersecções nos últimos 10 minutos a partir da
            -- timestamp_gps atual
            case
                when
                    count(
                        case
                            when
                                st_dwithin(
                                    shape,
                                    posicao_veiculo_geo,
                                    {{ var("tamanho_buffer_metros") }}
                                )
                            then 1
                        end
                    ) over (
                        partition by id_veiculo
                        order by
                            unix_seconds(timestamp(timestamp_gps))
                            range
                            between {{ var("intervalo_max_desvio_segundos") }} preceding
                            and current row
                    )
                    >= 1
                then true
                else false
            end as flag_trajeto_correto_hist,
            -- 3. Identificação de cadastro da linha no SIGMOB
            case
                when s.linha_gtfs is null then false else true
            end as flag_linha_existe_sigmob,
        -- 4. Join com data_versao_efetiva para definição de quais shapes serão
        -- considerados no cálculo das flags
        from registros r
        left join
            (
                select *
                from {{ ref("shapes_geom") }}
                where
                    id_modal_smtr in ({{ var("sppo_id_modal_smtr") | join(", ") }})
                    and data_versao = "{{ var('versao_fixa_sigmob')}}"
            ) s
            on r.linha = s.linha_gtfs
    )
-- 5. Agregação com LOGICAL_OR para evitar duplicação de registros
select
    id_veiculo,
    linha,
    linha_gtfs,
    route_id,
    data,
    timestamp_gps,
    logical_or(flag_trajeto_correto) as flag_trajeto_correto,
    logical_or(flag_trajeto_correto_hist) as flag_trajeto_correto_hist,
    logical_or(flag_linha_existe_sigmob) as flag_linha_existe_sigmob,
from intersec i
group by id_veiculo, linha, linha_gtfs, route_id, data, data_versao, timestamp_gps
