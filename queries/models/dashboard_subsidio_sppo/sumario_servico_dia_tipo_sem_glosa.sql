<<<<<<< staging/migra-apuracao-subsidio
with
    parametros as (
        select
            data_inicio,
            data_fim,
            max(
                if(status = "Licenciado sem ar e não autuado", subsidio_km, null)
            ) as subsidio_km_sem_ar_n_autuado,
            max(
                if(status = "Licenciado com ar e não autuado", subsidio_km, null)
            ) as subsidio_km_sem_glosa
        from {{ ref("subsidio_valor_km_tipo_viagem") }}
        where
            data_inicio >= "2023-07-04"
            and status
            in ("Licenciado sem ar e não autuado", "Licenciado com ar e não autuado")
        group by 1, 2
    )
select
    consorcio,
    data,
    tipo_dia,
    servico,
    viagens as viagens_subsidio,
    km_planejada as distancia_total_planejada,
    km_apurada as distancia_total_subsidio,
    null as valor_total_aferido,  -- TODO: Excluir essa coluna? é utilizada?
    perc_km_planejada as perc_distancia_total_subsidio,
    -- Valor total sem glosas: quando existe subsidio (POD>80%),  adiciona o valor
    -- glosado por tipo de viagem ao total
    case
        when perc_km_planejada >= 80
        then
            round(
                coalesce(valor_subsidio_pago, 0) + coalesce(
                    km_apurada_registrado_com_ar_inoperante * subsidio_km_sem_glosa, 0
                )
                + coalesce(km_apurada_autuado_ar_inoperante * subsidio_km_sem_glosa, 0)
                + coalesce(km_apurada_autuado_seguranca * subsidio_km_sem_glosa, 0)
                + coalesce(
                    km_apurada_autuado_limpezaequipamento * subsidio_km_sem_glosa, 0
                )
                + coalesce(
                    km_apurada_licenciado_sem_ar_n_autuado
                    * (subsidio_km_sem_glosa - subsidio_km_sem_ar_n_autuado),
                    0
                )
                + coalesce(km_apurada_n_vistoriado * subsidio_km_sem_glosa, 0)
                + coalesce(km_apurada_sem_transacao * subsidio_km_sem_glosa, 0),
                2
            )
        else 0
    end as valor_total_subsidio,
    coalesce(viagens_n_licenciado, 0) as viagens_n_licenciado,
    coalesce(km_apurada_n_licenciado, 0) as km_apurada_n_licenciado,
    coalesce(viagens_autuado_ar_inoperante, 0) as viagens_autuado_ar_inoperante,
    coalesce(km_apurada_autuado_ar_inoperante, 0) as km_apurada_autuado_ar_inoperante,
    coalesce(viagens_autuado_seguranca, 0) as viagens_autuado_seguranca,
    coalesce(km_apurada_autuado_seguranca, 0) as km_apurada_autuado_seguranca,
    coalesce(
        viagens_autuado_limpezaequipamento, 0
    ) as viagens_autuado_limpezaequipamento,
    coalesce(
        km_apurada_autuado_limpezaequipamento, 0
    ) as km_apurada_autuado_limpezaequipamento,
    coalesce(
        viagens_licenciado_sem_ar_n_autuado, 0
    ) as viagens_licenciado_sem_ar_n_autuado,
    coalesce(
        km_apurada_licenciado_sem_ar_n_autuado, 0
    ) as km_apurada_licenciado_sem_ar_n_autuado,
    coalesce(
        viagens_licenciado_com_ar_n_autuado, 0
    ) as viagens_licenciado_com_ar_n_autuado,
    coalesce(
        km_apurada_licenciado_com_ar_n_autuado, 0
    ) as km_apurada_licenciado_com_ar_n_autuado,
    coalesce(
        viagens_registrado_com_ar_inoperante, 0
    ) as viagens_registrado_com_ar_inoperante,
    coalesce(
        km_apurada_registrado_com_ar_inoperante, 0
    ) as km_apurada_registrado_com_ar_inoperante,
    coalesce(viagens_n_vistoriado, 0) as viagens_n_vistoriado,
    coalesce(km_apurada_n_vistoriado, 0) as km_apurada_n_vistoriado,
    coalesce(viagens_sem_transacao, 0) as viagens_sem_transacao,
    coalesce(km_apurada_sem_transacao, 0) as km_apurada_sem_transacao
from {{ ref("sumario_servico_dia_tipo") }}  -- `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_tipo`
left join parametros on data between data_inicio and data_fim
=======
WITH
  parametros AS (
  SELECT
    data_inicio,
    data_fim,
    MAX(IF(status = "Licenciado sem ar e não autuado", subsidio_km, NULL)) AS subsidio_km_sem_ar_n_autuado,
    MAX(IF(status = "Licenciado com ar e não autuado", subsidio_km, NULL)) AS subsidio_km_sem_glosa
  FROM
    {{ ref("subsidio_valor_km_tipo_viagem") }}
  WHERE
    data_inicio >= "2023-07-04"
    AND status IN ("Licenciado sem ar e não autuado", "Licenciado com ar e não autuado")
  GROUP BY
    1,
    2 )
SELECT
  consorcio,
  data,
  tipo_dia,
  servico,
  viagens AS viagens_subsidio,
  km_planejada AS distancia_total_planejada,
  km_apurada AS distancia_total_subsidio,
  NULL AS valor_total_aferido, -- TODO: Excluir essa coluna? é utilizada?
  perc_km_planejada AS perc_distancia_total_subsidio,
  -- Valor total sem glosas: quando existe subsidio (POD>80%),  adiciona o valor glosado por tipo de viagem ao total
  CASE
    WHEN perc_km_planejada >= 80 THEN ROUND(COALESCE(valor_subsidio_pago, 0) + COALESCE(km_apurada_registrado_com_ar_inoperante * subsidio_km_sem_glosa, 0) + COALESCE(km_apurada_autuado_ar_inoperante * subsidio_km_sem_glosa, 0) + COALESCE(km_apurada_autuado_seguranca * subsidio_km_sem_glosa, 0) + COALESCE(km_apurada_autuado_limpezaequipamento * subsidio_km_sem_glosa, 0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado * (subsidio_km_sem_glosa - subsidio_km_sem_ar_n_autuado), 0) + COALESCE(km_apurada_n_vistoriado * subsidio_km_sem_glosa, 0) + COALESCE(km_apurada_sem_transacao * subsidio_km_sem_glosa, 0), 2)
  ELSE
  0
END
  AS valor_total_subsidio,
  COALESCE(viagens_n_licenciado, 0) AS viagens_n_licenciado,
  COALESCE(km_apurada_n_licenciado, 0) AS km_apurada_n_licenciado,
  COALESCE(viagens_autuado_ar_inoperante, 0) AS viagens_autuado_ar_inoperante,
  COALESCE(km_apurada_autuado_ar_inoperante, 0) AS km_apurada_autuado_ar_inoperante,
  COALESCE(viagens_autuado_seguranca, 0) AS viagens_autuado_seguranca,
  COALESCE(km_apurada_autuado_seguranca, 0) AS km_apurada_autuado_seguranca,
  COALESCE(viagens_autuado_limpezaequipamento, 0) AS viagens_autuado_limpezaequipamento,
  COALESCE(km_apurada_autuado_limpezaequipamento, 0) AS km_apurada_autuado_limpezaequipamento,
  COALESCE(viagens_licenciado_sem_ar_n_autuado, 0) AS viagens_licenciado_sem_ar_n_autuado,
  COALESCE(km_apurada_licenciado_sem_ar_n_autuado, 0) AS km_apurada_licenciado_sem_ar_n_autuado,
  COALESCE(viagens_licenciado_com_ar_n_autuado, 0) AS viagens_licenciado_com_ar_n_autuado,
  COALESCE(km_apurada_licenciado_com_ar_n_autuado, 0) AS km_apurada_licenciado_com_ar_n_autuado,
  COALESCE(viagens_registrado_com_ar_inoperante, 0) AS viagens_registrado_com_ar_inoperante,
  COALESCE(km_apurada_registrado_com_ar_inoperante, 0) AS km_apurada_registrado_com_ar_inoperante,
  COALESCE(viagens_n_vistoriado, 0) AS viagens_n_vistoriado,
  COALESCE(km_apurada_n_vistoriado, 0) AS km_apurada_n_vistoriado,
  COALESCE(viagens_sem_transacao, 0) AS viagens_sem_transacao,
  COALESCE(km_apurada_sem_transacao, 0) AS km_apurada_sem_transacao
FROM
  {{ ref("sumario_servico_dia_tipo") }} -- `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_tipo`
LEFT JOIN
  parametros
ON
  DATA BETWEEN data_inicio
  AND data_fim
>>>>>>> master
