{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set staging_licenciamento_stu = ref("staging_licenciamento_stu") %}
{% set staging_veiculo_fiscalizacao_lacre = ref("staging_veiculo_fiscalizacao_lacre") %}

{% if execute and is_incremental() %}
    {% set licenciamento_previous_file_query %}
        select concat("'", ifnull(max(data_arquivo_fonte), date("{{ var('date_range_start') }}")), "'") as data
        from {{ this }}
        where data = date_sub(date("{{ var('date_range_start') }}"), interval 1 day)

    {% endset %}

    {% set licenciamento_previous_file = (
        run_query(licenciamento_previous_file_query).columns[0].values()[0]
    ) %}

    {% set inicio_vinculo_partitions_query %}
        with staging as (
            select date(data) as data, id_veiculo, placa, data_inicio_vinculo
            from {{ staging_licenciamento_stu }}
            where
                date(data) between date({{ licenciamento_previous_file }}) and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        novos_veiculos as (
            select *
            from staging
            qualify lag(data_inicio_vinculo) over(partition by id_veiculo, placa order by data) is null
        ),
        menor_inicio_vinculo as (
            select
                min(data_inicio_vinculo) as data_inicio_vinculo
            from novos_veiculos
            where data != date({{ licenciamento_previous_file }})
        )
        select distinct concat("'", data, "'") as data
        from menor_inicio_vinculo,
        unnest(generate_date_array(data_inicio_vinculo, date("{{ var('date_range_start') }}"), interval 1 day)) as data

    {% endset %}

    {% set inicio_vinculo_partitions = (
        run_query(inicio_vinculo_partitions_query).columns[0].values()
    ) %}

    {% set vistoria_partitions_query %}
        with staging as (
            select date(data) as data, id_veiculo, placa, data_ultima_vistoria
            from {{ staging_licenciamento_stu }}
            where
                date(data) between date({{ licenciamento_previous_file }}) and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        veiculo_chassi as (
            select distinct placa, trim(chassi) as chassi
            from {{ ref("staging_stu_veiculo") }}
            where chassi is not null
        ),
        licenciamento_chassi as (
            select
                l.* except (data_ultima_vistoria),
                v.chassi,
                coalesce(
                    l.data_ultima_vistoria,
                    last_value(l.data_ultima_vistoria ignore nulls) over w
                ) as data_ultima_vistoria
            from staging l
            left join veiculo_chassi v using (placa)
            window
                w as (
                    partition by l.id_veiculo, v.chassi
                    order by l.data
                    rows between unbounded preceding and current row
                )
        ),
        veiculos_vistoriados as (
            select *
            from licenciamento_chassi
            qualify
                (lag(data_ultima_vistoria) over(win) is null and data_ultima_vistoria is not null)
                or lag(data_ultima_vistoria) over(win) != data_ultima_vistoria
            window win as (partition by id_veiculo, chassi order by data)
        ),
        menor_data_vistoria as (
            select
                min(data_ultima_vistoria) as data_ultima_vistoria
            from veiculos_vistoriados
            where data != date({{ licenciamento_previous_file }})
        )
        select distinct concat("'", data, "'") as data
        from menor_data_vistoria,
        unnest(generate_date_array(data_ultima_vistoria, date("{{ var('date_range_start') }}"), interval 1 day)) as data

    {% endset %}
    {% set vistoria_partitions = (
        run_query(vistoria_partitions_query).columns[0].values()
    ) %}

    {% set lacre_partitions_query %}
        select distinct concat("'", data_lacre_deslacre, "'") as data
        from
            {{ staging_veiculo_fiscalizacao_lacre }},
            unnest(
                generate_date_array(
                    data_do_lacre,
                    coalesce(data_do_deslacre, date("{{ var('date_range_end') }}")),
                    interval 1 day
                )
            ) as data_lacre_deslacre
        where
            date(data) between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
        and data > '{{ var("data_final_veiculo_arquitetura_1") }}'

    {% endset %}
    {% set lacre_partitions = run_query(lacre_partitions_query).columns[0].values() %}

{% endif %}


with
    licenciamento_staging as (
        select
            date(data) as data,
            current_date("America/Sao_Paulo") as data_processamento,
            id_veiculo,
            placa,
            modo,
            permissao,
            ano_fabricacao,
            id_carroceria,
            id_interno_carroceria,
            carroceria,
            id_chassi,
            id_fabricante_chassi,
            nome_chassi,
            id_planta,
            tipo_veiculo,
            status,
            data_inicio_vinculo,
            data_ultima_vistoria,
            ano_ultima_vistoria,
            ultima_situacao,
            case
                when
                    data between "2025-11-12" and "2025-12-01"
                    and id_veiculo in (
                        'C50003',
                        'C50007',
                        'C50015',
                        'C50016',
                        'C50017',
                        'C50020',
                        'C50022',
                        'C50027',
                        'C50038',
                        'C50039',
                        'C50041',
                        'C50042',
                        'C50043',
                        'C50044',
                        'C50055',
                        'C50058',
                        'C50059',
                        'C50060',
                        'C50062',
                        'C50063',
                        'C50064',
                        'C50067',
                        'C50074',
                        'C50120',
                        'C50122',
                        'A41251',
                        'A41252',
                        'A41253',
                        'A41254',
                        'A41255',
                        'A41256',
                        'B27055',
                        'B27060',
                        'B27066',
                        'B27132',
                        'B27133',
                        'B27138',
                        'B27139',
                        'B27140',
                        'B44570',
                        'B44585',
                        'B44590',
                        'B44646',
                        'B44659',
                        'D13082',
                        'D13095',
                        'D17030',
                        'D17034',
                        'D17041',
                        'D17042',
                        'D17045',
                        'D17069',
                        'D17096',
                        'D17099',
                        'D86114',
                        'D86117',
                        'C27029',
                        'C27049',
                        'C27067',
                        'C27073',
                        'C27081',
                        'C27089',
                        'C27165',
                        'C27166',
                        'C27167',
                        'B27001',
                        'B27024',
                        'B27027',
                        'B27031',
                        'B27032',
                        'B27033',
                        'B27037',
                        'B27039',
                        'B27043',
                        'B27064',
                        'B27092',
                        'B27096',
                        'B27098',
                        'B27100',
                        'B27103',
                        'B27107',
                        'B27108',
                        'B27111',
                        'B27114',
                        'B27134',
                        'B27135',
                        'B27137',
                        'B27143',
                        'B27144',
                        'B27145',
                        'B27146',
                        'B27147',
                        'B27148',
                        'B27149',
                        'B27150',
                        'B27152',
                        'B27155',
                        'B27156',
                        'B27157',
                        'B27160',
                        'B27162',
                        'B27163',
                        'B27164',
                        'B27184',
                        'B27185',
                        'B27187',
                        'B27188',
                        'B27189',
                        'B27190',
                        'B27230',
                        'B27231',
                        'B27232',
                        'B27233',
                        'B27234',
                        'B27235',
                        'B27236',
                        'B27237',
                        'B27238',
                        'B27239',
                        'B27240',
                        'B27241',
                        'B27242',
                        'B27243',
                        'B27244',
                        'B27245',
                        'B27246',
                        'B27247',
                        'B27248',
                        'B27249',
                        'B27250',
                        'B27251',
                        'D33028',
                        'D33029',
                        'D33030',
                        'D33031',
                        'D33032',
                        'D33033',
                        'D33034',
                        'D33035',
                        'D33036',
                        'D33037',
                        'D33038',
                        'D33039',
                        'D33041',
                        'D33042',
                        'D33043',
                        'D33044',
                        'D33063',
                        'D33064',
                        'D86005',
                        'D86008',
                        'D86010',
                        'D86018',
                        'D86021',
                        'D86022',
                        'D86024',
                        'D86025',
                        'D86026',
                        'D86028',
                        'D86031',
                        'D86032',
                        'D86034',
                        'D86035',
                        'D86036',
                        'D86037',
                        'D86038',
                        'D86039',
                        'D86040',
                        'D86041',
                        'D86043',
                        'D86044',
                        'D86046',
                        'D86047',
                        'D86049',
                        'D86051',
                        'D86054',
                        'D86057',
                        'D86059',
                        'D86061',
                        'D86070',
                        'D86075',
                        'D86076',
                        'D86077',
                        'D86079',
                        'D86081',
                        'D86082',
                        'D86083',
                        'D86084',
                        'D86086',
                        'D86088',
                        'D86090',
                        'D86091',
                        'D86092',
                        'D86093',
                        'D86094',
                        'D86095',
                        'D86097',
                        'D86101',
                        'D86102',
                        'D86103',
                        'D86110',
                        'D86113',
                        'D86118',
                        'D86119',
                        'D86120',
                        'D86122',
                        'D86125',
                        'D86127',
                        'D86129',
                        'D86133',
                        'D86136',
                        'D86144',
                        'D86145',
                        'D86148',
                        'D86155',
                        'D86157',
                        'D86165',
                        'D86168',
                        'D86171',
                        'D86172',
                        'D86173',
                        'D86174',
                        'D86175',
                        'D86180',
                        'D86181',
                        'D86182',
                        'D86185',
                        'D86186',
                        'D86218',
                        'D86236',
                        'D86254',
                        'D86255',
                        'D86257',
                        'D86258',
                        'D86259',
                        'D86263',
                        'D86264',
                        'D86265',
                        'D86266',
                        'D86267',
                        'D86268',
                        'D86269',
                        'D86271',
                        'D86272',
                        'D86273',
                        'D86274',
                        'D86275',
                        'D86276',
                        'D86277',
                        'D86278',
                        'D86279',
                        'D86289',
                        'D86290',
                        'D86291',
                        'D86292',
                        'D86293',
                        'D86294',
                        'D86295',
                        'D86296',
                        'D86297',
                        'D86298',
                        'D86299',
                        'D86300',
                        'D86301',
                        'D86302',
                        'D86303',
                        'D86304',
                        'D86305',
                        'D86306',
                        'D86307',
                        'D86308',
                        'D86309',
                        'D86310',
                        'D86311',
                        'D86312',
                        'D86313',
                        'D86314',
                        'D86316',
                        'B32513',
                        'B32541',
                        'B32542',
                        'B32525',
                        'B32547',
                        'B32670',
                        'B32668',
                        'B32644',
                        'B32502',
                        'B32503',
                        'B32506',
                        'B32523',
                        'B32525',
                        'B32526',
                        'B32547',
                        'B32548',
                        'B32551',
                        'B32552',
                        'B32554',
                        'B32555',
                        'B32612',
                        'B32616',
                        'B32644',
                        'B32654',
                        'B32668',
                        'B32669',
                        'B32670',
                        'B32672',
                        'B32679',
                        'B32684',
                        'B32691',
                        'B32698',
                        'B32699',
                        'B32700',
                        'B32701',
                        'B32708',
                        'B32715',
                        'D33198',
                        'D33200',
                        'D33199'
                    )
                then "BASICO"  -- MTR-CAP-2025/59482
                when
                    data between "2025-11-20" and "2025-12-16"
                    and id_veiculo in (
                        'A29084',
                        'A29092',
                        'A29106',
                        'A29171',
                        'B27007',
                        'B27125',
                        'B27130',
                        'B27136',
                        'C27129'

                    )
                then "BASICO"  -- MTR-CAP-2025/59482
                when tipo_veiculo like "%BASIC%" or tipo_veiculo like "%BS%"
                then "BASICO"
                when tipo_veiculo like "%MIDI%"
                then "MIDI"
                when tipo_veiculo like "%MINI%"
                then "MINI"
                when tipo_veiculo like "%PDRON%" or tipo_veiculo like "%PADRON%"
                then "PADRON"
                when tipo_veiculo like "%ARTICULADO%"
                then "ARTICULADO"
            end as tecnologia,
            quantidade_lotacao_pe,
            quantidade_lotacao_sentado,
            tipo_combustivel,
            indicador_ar_condicionado,
            indicador_elevador,
            indicador_usb,
            indicador_wifi,
            min(date(data)) over (order by data) as primeira_data,
            date(data) as data_arquivo_fonte
        from {{ ref("staging_licenciamento_stu") }}
        where
            data > '{{ var("data_final_veiculo_arquitetura_1") }}'
            {% if is_incremental() %}
                and date(data) between date({{ licenciamento_previous_file }}) and date(
                    "{{ var('date_range_end') }}"
                )
            {% endif %}
    ),
    veiculo_chassi as (
        select distinct placa, trim(chassi) as chassi
        from {{ ref("staging_stu_veiculo") }}
        where chassi is not null
    ),
    licenciamento_chassi as (
        select
            l.* except (data_ultima_vistoria, ano_ultima_vistoria),
            v.chassi,
            lag(date(l.data)) over (
                partition by l.id_veiculo, v.chassi order by l.data
            ) as ultima_data,
            coalesce(
                l.data_ultima_vistoria,
                last_value(l.data_ultima_vistoria ignore nulls) over w
            ) as data_ultima_vistoria,
            coalesce(
                l.ano_ultima_vistoria,
                last_value(l.ano_ultima_vistoria ignore nulls) over w
            ) as ano_ultima_vistoria,
            case
                when
                    l.data_ultima_vistoria is null
                    and last_value(l.data_ultima_vistoria ignore nulls) over w
                    is not null
                then true
                else false
            end as indicador_data_ultima_vistoria_tratada
        from licenciamento_staging l
        left join veiculo_chassi v using (placa)
        window
            w as (
                partition by l.id_veiculo, v.chassi
                order by l.data
                rows between unbounded preceding and current row
            )
    ),
    datas_faltantes as (
        select distinct
            data,
            max(l.data_arquivo_fonte) over (
                order by data rows unbounded preceding
            ) as data_arquivo_fonte
        from
            unnest(
                {% if is_incremental() %}
                    generate_date_array(
                        date('{{ var("date_range_start") }}'),
                        date('{{ var("date_range_end") }}'),
                        interval 1 day
                    )
                {% else %}
                    generate_date_array(
                        '{{ var("data_final_veiculo_arquitetura_1") }}',
                        current_date("America/Sao_Paulo"),
                        interval 1 day
                    )
                {% endif %}
            ) as data
        full outer join licenciamento_chassi l using (data)
        where data > '{{ var("data_final_veiculo_arquitetura_1") }}'
    ),
    licenciamento_datas_preenchidas as (
        select df.data, l.* except (data)
        from licenciamento_chassi l
        left join datas_faltantes df using (data_arquivo_fonte)
    ),
    veiculo_fiscalizacao_lacre as (
        select * from {{ ref("veiculo_fiscalizacao_lacre") }}
    ),
    inicio_vinculo_preenchido as (
        select data_corrigida as data, s.* except (data)
        from
            licenciamento_chassi s,
            unnest(
                generate_date_array(s.data_inicio_vinculo, data, interval 1 day)
            ) as data_corrigida
        where
            s.ultima_data is null
            and s.data != s.primeira_data
            and data_corrigida > '{{ var("data_final_veiculo_arquitetura_1") }}'
    ),
    dados_novos as (
        select *
        from
            (
                select *
                from inicio_vinculo_preenchido
                union all
                select *
                from licenciamento_datas_preenchidas
            )
        qualify
            row_number() over (
                partition by data, id_veiculo
                order by data_inicio_vinculo desc, data_arquivo_fonte asc
            )
            = 1
    ),
    {% if is_incremental() %}
        dados_atuais as (
            select *
            from {{ this }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
                {% if lacre_partitions | length > 0 %}
                    or data in ({{ lacre_partitions | join(", ") }})

                {% endif %}
                {% if inicio_vinculo_partitions | length > 0 %}
                    or data in ({{ inicio_vinculo_partitions | join(", ") }})

                {% endif %}
                {% if vistoria_partitions | length > 0 %}
                    or data in ({{ vistoria_partitions | join(", ") }})

                {% endif %}
        ),
        dados_novos_lacre_vistoria as (
            select * except (chassi, ultima_data, primeira_data)
            from dados_novos

            {% if lacre_partitions | length > 0 or vistoria_partitions | length > 0 %}
                union all by name

                select
                    da.data,
                    current_date("America/Sao_Paulo") as data_processamento,
                    da.* except (
                        data,
                        data_processamento,
                        indicador_veiculo_lacrado,
                        versao,
                        datetime_ultima_atualizacao,
                        id_execucao_dbt
                    )
                from dados_atuais da
                left join
                    (
                        select distinct data, id_veiculo, placa from dados_novos
                    ) dn using (data, id_veiculo, placa)
                where
                    (
                        {% if lacre_partitions | length > 0 %}
                            data in ({{ lacre_partitions | join(", ") }})
                        {% else %}data = '2000-01-01'
                        {% endif %}
                        or {% if vistoria_partitions | length > 0 %}
                            data in ({{ vistoria_partitions | join(", ") }})
                        {% else %} data = '2000-01-01'
                        {% endif %}
                    )
                    and dn.data is null
                qualify
                    row_number() over (
                        partition by data, id_veiculo, placa
                        order by data_processamento desc
                    )
                    = 1

            {% endif %}
        )
    {% else %} dados_novos_lacre_vistoria as (select * from dados_novos)
    {% endif %},
    veiculo_lacrado as (
        select
            dn.*,
            vfl.id_veiculo is not null as indicador_veiculo_lacrado,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from dados_novos_lacre_vistoria dn
        left join
            veiculo_fiscalizacao_lacre vfl
            on dn.id_veiculo = vfl.id_veiculo
            and dn.placa = vfl.placa
            and dn.data >= vfl.data_inicio_lacre
            and (dn.data < vfl.data_fim_lacre or vfl.data_fim_lacre is null)
    ),
    veiculo_lacrado_deduplicado as (
        select *
        from veiculo_lacrado
        qualify
            row_number() over (
                partition by data, id_veiculo, placa
                order by indicador_veiculo_lacrado desc
            )
            = 1
    ),
    data_vistoria_atualizacao as (
        select
            date(data) as data,
            id_veiculo,
            placa,
            chassi,
            data_ultima_vistoria,
            ano_ultima_vistoria
        from dados_novos
        qualify
            (
                lag(data_ultima_vistoria) over (win) is null
                and data_ultima_vistoria is not null
            )
            or lag(data_ultima_vistoria) over (win) != data_ultima_vistoria
        window win as (partition by id_veiculo, chassi order by data)

    ),
    nova_data_ultima_vistoria as (
        select
            nova_data as data,
            id_veiculo,
            placa,
            chassi,
            data_ultima_vistoria,
            ano_ultima_vistoria
        from
            data_vistoria_atualizacao d,
            unnest(
                generate_date_array(data_ultima_vistoria, data, interval 1 day)
            ) as nova_data
        qualify
            row_number() over (
                partition by nova_data, id_veiculo, chassi order by d.data desc
            )
            = 1
    ),
    veiculo_vistoriado as (
        select
            v.* except (data_ultima_vistoria, ano_ultima_vistoria),
            ifnull(
                d.data_ultima_vistoria, v.data_ultima_vistoria
            ) as data_ultima_vistoria,
            ifnull(d.ano_ultima_vistoria, v.ano_ultima_vistoria) as ano_ultima_vistoria
        from veiculo_lacrado_deduplicado v
        left join nova_data_ultima_vistoria d using (data, id_veiculo, placa)
    ),
    final as (
        {% if is_incremental() %}

            with
                dados_completos as (
                    select *, 1 as priority
                    from dados_atuais

                    union all by name

                    select *, cast(null as string) as id_execucao_dbt, 0 as priority
                    from veiculo_vistoriado
                ),
                dados_completos_sha as (
                    {% set columns = (
                        list_columns()
                        | reject(
                            "in",
                            [
                                "data_processamento",
                                "versao",
                                "datetime_ultima_atualizacao",
                                "id_execucao_dbt",
                            ],
                        )
                        | list
                    ) %}

                    select
                        *,
                        sha256(
                            concat(
                                {% for c in columns %}
                                    ifnull(cast({{ c }} as string), 'n/a')
                                    {% if not loop.last %}, {% endif %}
                                {% endfor %}
                            )
                        ) as sha_dado
                    from dados_completos
                ),
                dados_completos_invocation_id as (
                    select
                        * except (id_execucao_dbt),
                        case
                            when
                                lag(sha_dado) over (win) != sha_dado
                                or (
                                    lag(sha_dado) over (win) is null
                                    and count(*) over (win) = 1
                                )
                            then '{{ invocation_id }}'
                            else
                                ifnull(id_execucao_dbt, lag(id_execucao_dbt) over (win))
                        end as id_execucao_dbt
                    from dados_completos_sha
                    window
                        win as (
                            partition by data, id_veiculo, placa, data_processamento
                            order by priority desc
                        )

                ),
                dados_completos_deduplicados as (
                    select *
                    from dados_completos_invocation_id
                    qualify
                        row_number() over (
                            partition by data, data_processamento, id_veiculo, placa
                            order by priority
                        )
                        = 1
                )
            select * except (sha_dado)
            from dados_completos_deduplicados
            qualify
                lag(sha_dado) over (win) != sha_dado or lag(sha_dado) over (win) is null
            window
                win as (
                    partition by data, id_veiculo, placa order by data_processamento
                )
        {% else %}
            select *, '{{ invocation_id }}' as id_execucao_dbt from veiculo_vistoriado
        {% endif %}
    )
select
    data,
    data_processamento,
    id_veiculo,
    placa,
    modo,
    permissao,
    ano_fabricacao,
    id_carroceria,
    id_interno_carroceria,
    carroceria,
    id_chassi,
    id_fabricante_chassi,
    nome_chassi,
    id_planta,
    tipo_veiculo,
    status,
    data_inicio_vinculo,
    data_ultima_vistoria,
    ano_ultima_vistoria,
    ultima_situacao,
    tecnologia,
    quantidade_lotacao_pe,
    quantidade_lotacao_sentado,
    tipo_combustivel,
    indicador_ar_condicionado,
    indicador_elevador,
    indicador_usb,
    indicador_wifi,
    indicador_veiculo_lacrado,
    indicador_data_ultima_vistoria_tratada,
    data_arquivo_fonte,
    versao,
    datetime_ultima_atualizacao,
    id_execucao_dbt
from final
where data <= data_processamento
