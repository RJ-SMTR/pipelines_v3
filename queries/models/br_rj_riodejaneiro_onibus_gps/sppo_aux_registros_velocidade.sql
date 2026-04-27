{{ config(materialized="ephemeral") }}
/*
Descrição:
Estimativa das velocidades dos veículos nos últimos 10 minutos contados a partir da timestamp_gps atual.
Essa metodologia serve para determinar quais carros estão em movimento e quais estão parados.
1. Calculamos a velocidade do veículo no último trecho de 10 minutos de operação.
A implementação utiliza a função 'first_value' com uma janela (cláusula 'over') de até 10 minutos anteriores à
timestamp_gps atual e calcula a distância do ponto mais antigo (o first_value na janela) ao ponto atual (posicao_veiculo_geo).
Dividimos essa distância pela diferença de tempo entre a timestamp_gps atual e a timestamp_gps do ponto mais
antigo da janela (o qual recuperamos novamente com o uso de first_value).
Esta diferença de tempo (datetime_diff) é calculada em segundos, portanto multiplicamos o resultado da divisão por um fator
3.6 para que a velocidade esteja em quilômetros por hora. O resultado final é arrendondado sem casas decimais.
Por fim, cobrimos esse cálculo com a função 'if_null' e retornamos zero para a velocidade em casos onde a divisão retornaria
um valor nulo.
2. Após o calculo da velocidade, definimos a coluna 'status_movimento'. Veículos abaixo da 'velocidade_limiar_parado', são
considerados como 'parado'. Caso contrário, são considerados 'andando'
*/
with
    t_velocidade as (
        select
            data,
            id_veiculo,
            timestamp_gps,
            linha,
            st_distance(
                posicao_veiculo_geo,
                lag(posicao_veiculo_geo) over (
                    partition by id_veiculo, linha order by timestamp_gps
                )
            ) distancia,
            ifnull(
                safe_divide(
                    st_distance(
                        posicao_veiculo_geo,
                        lag(posicao_veiculo_geo) over (
                            partition by id_veiculo, linha order by timestamp_gps
                        )
                    ),
                    datetime_diff(
                        timestamp_gps,
                        lag(timestamp_gps) over (
                            partition by id_veiculo, linha order by timestamp_gps
                        ),
                        second
                    )
                ),
                0
            )
            * 3.6 velocidade
        from {{ ref("sppo_aux_registros_filtrada") }}
        {% if not flags.FULL_REFRESH -%}
            where
                data between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and timestamp_gps > "{{var('date_range_start')}}"
                and timestamp_gps <= "{{var('date_range_end')}}"
        {%- endif -%}
    ),
    medias as (
        select
            data,
            id_veiculo,
            timestamp_gps,
            linha,
            distancia,
            velocidade,  -- velocidade do pontual
            avg(velocidade) over (
                partition by id_veiculo, linha
                order by
                    unix_seconds(timestamp(timestamp_gps))
                    range
                    between {{ var("janela_movel_velocidade") }} preceding
                    and current row
            ) velocidade_media  -- velocidade com média móvel
        from t_velocidade
    )
select
    timestamp_gps,
    data,
    id_veiculo,
    linha,
    distancia,
    round(
        case
            when velocidade_media > {{ var("velocidade_maxima") }}
            then {{ var("velocidade_maxima") }}
            else velocidade_media
        end,
        1
    ) as velocidade,
    -- 2. Determinação do estado de movimento do veículo.
    case
        when velocidade_media < {{ var("velocidade_limiar_parado") }}
        then false
        else true
    end flag_em_movimento,
from medias
