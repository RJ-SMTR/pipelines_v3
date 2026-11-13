-- Vehicle details view: one row per vehicle per licensing day.
-- Source: rj-smtr.cadastro.veiculo_licenciamento_dia (data from 2025-04-01), which
-- replaced the removed rj-smtr.veiculo.sppo_licenciamento. The old table held SPPO
-- buses only, so the same filter is applied here. The new table can hold several
-- processing versions per vehicle and day, so the latest one is kept.
select
    data as service_date,
    case
        when data = max(data) over (partition by id_veiculo) then 'TRUE' else 'FALSE'
    end as latest_capture,
    modo as mode,
    id_veiculo as vehicle_id,
    ano_fabricacao as manufacture_year,
    carroceria as body_model,
    data_ultima_vistoria as last_inspection_date,
    id_carroceria as body_id,
    id_chassi as chassis_id,
    id_fabricante_chassi as chassis_manufacturer_id,
    id_interno_carroceria as internal_bodywork_id,
    id_planta as plant_id,
    indicador_ar_condicionado as aircon_indicator,
    indicador_elevador as elevator_indicator,
    indicador_usb as usb_indicator,
    indicador_wifi as wifi_indicator,
    nome_chassi as chassis_name,
    permissao as permit_number,
    placa as plate_id,
    quantidade_lotacao_pe as passengers_standing,
    quantidade_lotacao_sentado as passengers_sitting,
    tipo_combustivel as fuel_type,
    tipo_veiculo as vehicle_type,
    status as status
from `rj-smtr.cadastro.veiculo_licenciamento_dia`
where modo = 'ONIBUS' and tipo_veiculo not like '%ROD%'
qualify
    row_number() over (partition by data, id_veiculo order by data_processamento desc)
    = 1
