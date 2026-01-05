{% test test_check_veiculo_lacre(model) %}
    with
        veiculo_dia as (
            select
                data,
                id_veiculo,
                placa,
                json_value(indicadores, '$.indicador_veiculo_lacrado.valor')
                = 'true' as indicador_veiculo_lacrado,
                safe_cast(json_value(indicadores, '$.indicador_veiculo_lacrado.data_processamento_licenciamento') as date) as data_processamento_licenciamento
            from {{ model }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

        ),
        veiculo_licenciamento_dia as (
          select data, id_veiculo, placa, indicador_veiculo_lacrado, data_processamento
          from {{ ref('veiculo_licenciamento_dia') }}
          where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        veiculo_fiscalizacao_lacre as (
            select data_inicio_lacre, data_fim_lacre, id_veiculo, placa
            from {{ ref("veiculo_fiscalizacao_lacre") }}
            where
                data_inicio_lacre <= date("{{ var('date_range_end') }}")
                and (
                    data_fim_lacre >= date("{{ var('date_range_start') }}")
                    or data_fim_lacre is null
                )
        ),
        veiculos_lacrados_periodo as (
            select distinct
                vfl.id_veiculo,
                vfl.placa,
                generate_date_array(
                    greatest(
                        vfl.data_inicio_lacre, date("{{ var('date_range_start') }}")
                    ),
                    case
                        when vfl.data_fim_lacre is null
                        then date("{{ var('date_range_end') }}")
                        else
                            least(
                                date_sub(vfl.data_fim_lacre, interval 1 day),
                                date("{{ var('date_range_end') }}")
                            )
                    end
                ) as datas_lacre
            from veiculo_fiscalizacao_lacre vfl
        ),
        veiculos_lacrados_expandido as (
            select id_veiculo, placa, data_lacre
            from veiculos_lacrados_periodo, unnest(datas_lacre) as data_lacre
        ),
        -- Teste 1: Verificar se todos os veículos lacrados estão na tabela
        -- veiculo_fiscalizacao_lacre
        teste_1_falha as (
            select
                vd.data,
                vd.id_veiculo,
                vd.placa,
                'veiculo_dia indica lacrado mas não encontrado em veiculo_fiscalizacao_lacre'
                as erro
            from veiculo_dia vd
            left join
                veiculos_lacrados_expandido vle
                on vd.id_veiculo = vle.id_veiculo
                and vd.data = vle.data_lacre
                and vd.placa = vle.placa
                left join veiculo_licenciamento_dia vld
             on vd.id_veiculo = vld.id_veiculo
                and vd.data = vld.data
                and vd.placa = vld.placa
                and vd.data_processamento_licenciamento = vld.data_processamento
            where vd.indicador_veiculo_lacrado is true and vle.id_veiculo is null
            and vld.indicador_veiculo_lacrado is false
        ),
        -- Teste 2: Verificar se todos os veículos lacrados da
        -- veiculo_fiscalizacao_lacre estão marcados como lacrados na veiculo_dia
        teste_2_falha as (
            select
                vle.data_lacre as data,
                vle.id_veiculo,
                vle.placa,
                'veiculo_fiscalizacao_lacre indica lacrado mas veiculo_dia não' as erro
            from veiculos_lacrados_expandido vle
            left join
                veiculo_dia vd
                on vle.id_veiculo = vd.id_veiculo
                and vle.data_lacre = vd.data
                and vle.placa = vd.placa
            left join veiculo_licenciamento_dia vld
                on vd.id_veiculo = vld.id_veiculo
                and vd.data = vld.data
                and vd.placa = vld.placa
                and vd.data_processamento_licenciamento = vld.data_processamento
            where vd.indicador_veiculo_lacrado is false
            and vld.indicador_veiculo_lacrado is true
        ),
        falhas as (
            select data, id_veiculo, placa, erro
            from teste_1_falha

            union all

            select data, id_veiculo, placa, erro
            from teste_2_falha
        )

    select *
    from falhas
{% endtest %}
