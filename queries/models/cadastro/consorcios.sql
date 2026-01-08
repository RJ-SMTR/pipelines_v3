{{ config(materialized="table") }}

with
    stu as (
        select
            perm_autor as id_consorcio,
            cnpj,
            processo,
            data_registro,
            razao_social,
            case
                {% for id_stu, id_jae in var("ids_consorcios").items() %}
                    when perm_autor = {{ id_stu }} then {{ id_jae }}
                {% endfor %}
            end as cd_consorcio_jae
        from {{ ref("staging_operadora_empresa") }} as stu
        where perm_autor in ({{ var("ids_consorcios").keys() | join(", ") }})
    ),
    consorcio as (
        select
            coalesce(s.id_consorcio, j.cd_consorcio) as id_consorcio,
            case
                when s.id_consorcio = '221000050'
                then "Consórcio BRT"
                else j.nm_consorcio
            end as consorcio,
            s.cnpj,
            s.razao_social,
            s.id_consorcio as id_consorcio_stu,
            j.cd_consorcio as id_consorcio_jae,
            s.processo as id_processo
        from {{ ref("staging_consorcio") }} as j
        full outer join stu as s on j.cd_consorcio = s.cd_consorcio_jae
    )
select
    c.id_consorcio,
    c.consorcio,
    m.modo,
    c.cnpj,
    c.razao_social,
    c.id_consorcio_stu,
    c.id_consorcio_jae,
    c.id_processo
from consorcio c
left join {{ ref("aux_consorcio_modo") }} as cm using (id_consorcio)
left join
    {{ ref("modos") }} as m on m.id_modo = cm.id_modo and cm.fonte_id_modo = m.fonte
