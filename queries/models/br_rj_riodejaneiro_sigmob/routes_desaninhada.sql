with
    desaninhada as (
        select
            data_versao,
            json_value(content, '$.agency_id') agency_id,
            json_value(content, '$.route_short_name') route_short_name,
            json_value(content, '$.route_long_name') route_long_name,
            json_value(content, '$.route_desc') route_desc,
            json_value(content, '$.route_type') route_type,
            json_value(content, '$.route_url') route_url,
            json_value(content, '$.route_color') route_color,
            json_value(content, '$.route_text_color') route_text_color,
            json_value(content, '$.ATIVA') ativa,
            json_value(content, '$.ATUALIZADO') atualizado,
            json_value(content, '$.Descricao') descricao,
            json_value(content, '$.idModalSmtr') idmodalsmtr,
            json_value(content, '$.linha_id') linha_id,
            json_value(content, '$.brs') brs,
            json_value(content, '$.IDTipoServico') idtiposervico,
            json_value(content, '$.IDVariacaoServico') idvariacaoservico,
            json_value(content, '$.origem') origem,
            json_value(content, '$.destino') destino,
            json_value(content, '$.ClassificacaoEspacial') classificacaoespacial,
            json_value(content, '$.ClassificacaoHierarquica') classificacaohierarquica,
            json_value(content, '$.InicioVigencia') iniciovigencia,
            json_value(content, '$.LegisInicioVigencia') legisiniciovigencia,
            json_value(content, '$.fimVigencia') fimvigencia,
            json_value(content, '$.LegisfimVigencia') legisfimvigencia,
            json_value(content, '$.FlagVigente') flagvigente,
            json_value(content, '$.FrotaDeterminada') frotadeterminada,
            json_value(content, '$.LegisFrota') legisfrota,
            json_value(content, '$.FrotaServico') frotaservico,
            json_value(content, '$.FrotaOperante') frotaoperante,
            json_value(content, '$.Observacoes') observacoes,
            json_value(content, '$.IDParadaOrigem') idparadaorigem,
            json_value(content, '$.SiglaServico') siglaservico,
            json_value(content, '$.IDParadaDestino') idparadadeestino,
            json_value(content, '$.route_id') route_id,
            json_value(content, '$.id') id,
            json_value(content, '$.agency_name') agency_name,
            json_value(content, '$.Via') via,
            json_value(content, '$.Vista') vista,
            json_value(content, '$.Complemento') complemento,
            json_value(content, '$.OLD_routes_id') old_route_id
        from {{ ref("routes") }}
    ),
    ultimas_versoes as (
        select *
        from desaninhada
        where date(fimvigencia) >= date(data_versao) or fimvigencia is null
        order by route_id, fimvigencia
    )
select *
from ultimas_versoes
