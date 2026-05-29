{{
    config(
        materialized="view",
    )
}}Expand commentComment on lines R1 to R5Resolved

with
    dados_consolidados as (

        select
            data,
            id_veiculo,
            ano_fabricacao,
            carroceria,
            indicador_ar_condicionado,
            nome_chassi,
            permissao,
            placa,
            tipo_veiculo,
            safe_cast(substr(id_veiculo, 2, 3) as int64) as cod_veiculo
        from {{ ref("sppo_licenciamento") }}
        where
            permissao in ("22.100002-3", "22.100003-2", "22.100001-4", "22.100004-1")
            and data < date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
            and lower(tipo_veiculo) not like "%rod%"Expand commentComment on lines R22 to R25Resolved

        union all

        select
            data,
            id_veiculo,
            ano_fabricacao,
            carroceria,
            indicador_ar_condicionado,
            nome_chassi,
            permissao,
            placa,
            tipo_veiculo,
            safe_cast(substr(id_veiculo, 2, 3) as int64) as cod_veiculo
        from {{ ref("veiculo_licenciamento_dia") }}
        where
            permissao in ("22.100002-3", "22.100003-2", "22.100001-4", "22.100004-1")
            and data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
            and lower(tipo_veiculo) not like "%rod%"
        qualify
            row_number() over (
                partition by data, id_veiculo order by data_processamento desc
            )
            = 1Expand commentComment on lines R27 to R49Resolved
    ),

    operadoras_onibus as (
        select distinct id_operadora, operadora
        from {{ ref("operadoras") }}
        where modo = "Ônibus"
    ),Expand commentComment on lines R52 to R56Resolved

    classificacao_operadora as (
        select
            d.*,

            case
                when cod_veiculo between 100 and 104
                then '220100007'
                when cod_veiculo between 105 and 109
                then '220105002'
                when cod_veiculo between 115 and 119
                then '220115003'
                when cod_veiculo between 120 and 124
                then '220120000'
                when cod_veiculo between 125 and 129
                then '220125004'
                when ((cod_veiculo between 130 and 139) and (cod_veiculo != 134))
                then '220130000'
                when cod_veiculo between 170 and 179
                then '31'
                when cod_veiculo between 180 and 189
                then '2645'
                when cod_veiculo between 255 and 259
                then '220255006'
                when cod_veiculo between 270 and 274
                then '220270003'
                when cod_veiculo between 275 and 279
                then '220275008'
                when cod_veiculo between 285 and 289
                then '220285009'
                when cod_veiculo between 290 and 299
                then '220290005'
                when cod_veiculo between 300 and 309
                then '220300005'
                when cod_veiculo between 310 and 319
                then '220310006'
                when cod_veiculo between 325 and 329
                then '220325002'
                when cod_veiculo between 330 and 339
                then '2677'
                when cod_veiculo between 410 and 419
                then '220410005'
                when cod_veiculo between 445 and 449
                then '220445003'
                when cod_veiculo between 470 and 479
                then '220475006'
                when cod_veiculo between 480 and 489
                then '220480002'
                when cod_veiculo between 500 and 509
                then '220500003'
                when cod_veiculo between 515 and 519
                then '220515009'
                when cod_veiculo between 535 and 539
                then '220535001'
                when cod_veiculo between 580 and 589
                then '220580001'
                when cod_veiculo between 630 and 639
                then '220630005'
                when cod_veiculo between 710 and 714
                then '220710002'
                when cod_veiculo between 715 and 719
                then '220715007'
                when cod_veiculo between 720 and 729
                then '220720003'
                when cod_veiculo between 860 and 869
                then '220860006'
                when cod_veiculo between 870 and 879
                then '220870007'
                else null
            end as id_operadora

        from dados_consolidados d
    )

select
    c.data,
    c.id_veiculo,
    c.ano_fabricacao,
    c.carroceria,
    c.indicador_ar_condicionado,
    c.nome_chassi,
    c.permissao,
    c.placa,
    c.tipo_veiculo,
    c.cod_veiculo,
    o.operadora

from classificacao_operadora c

left join operadoras_onibus o using (id_operadora)