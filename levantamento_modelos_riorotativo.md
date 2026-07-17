# Levantamento de tabelas para os modelos dbt do Rio Rotativo

Este documento consolida o levantamento realizado a partir de:

- documento Google Docs `Rio Rotativo - Regras de Negócio_REV01`;
- comentários e respostas existentes no documento;
- modelo relacional apresentado no arquivo `Modelo Estacionamento.pdf`;
- modelos e capturas já existentes no repositório.

O modelo `dashboard_riorotativo_atividade_viaria` está fora do escopo deste
levantamento.

## Modelos previstos

### `verificacao_guardador_veiculo`

Modelo granular com uma linha por verificação realizada por um guardador sobre
um veículo.

Deve concentrar:

- identificação da verificação;
- CPF do guardador;
- placa verificada;
- período de estacionamento associado;
- classificação da verificação como válida ou inválida para repasse;
- motivo de não repasse;
- valores arrecadados;
- valores de repasse;
- informações necessárias para auditoria do processamento.

### `ordem_pagamento_riorotativo_guardador_veiculo_dia`

Modelo agregado com uma linha por guardador por dia, construído a partir de
`verificacao_guardador_veiculo`.

Deve concentrar:

- quantidade total de verificações;
- quantidade de verificações válidas;
- quantidade de verificações inválidas;
- valor diário devido ao guardador.

Não haverá pagamento mínimo nem máximo. Portanto, o campo
`indicador_pagamento_minimo` não deve fazer parte da especificação inicial.

### `ordem_pagamento_riorotativo_entidade_dia`

Modelo agregado com uma linha por entidade por dia.

Cada entidade à qual o guardador estiver vinculado deve receber R$ 0,11 por
verificação válida. Caso o guardador esteja vinculado a mais de uma entidade,
cada entidade recebe o valor correspondente.

O vínculo entre guardador e entidade deve respeitar a vigência existente na data
da fiscalização.

### Modelo de movimentações/ativações

Deverá ser criado um quarto modelo para permitir o acompanhamento das
movimentações, ativações e receita do estacionamento rotativo.

A especificação inicial poderá partir de um dos seguintes grãos:

- uma linha por movimento;
- uma linha por ativação completa;
- uma linha por área por dia.

O grão definitivo ainda deverá ser escolhido antes da implementação desse
modelo. Como ponto de partida técnico, uma linha por movimento preserva o maior
nível de detalhe e permite produzir posteriormente os demais agregados.

## Tabelas-fonte necessárias

| Tabela | Necessidade | Uso |
| --- | --- | --- |
| `fiscalizacao_veiculo` | Obrigatória | Verificação, CPF do guardador, placas OCR e digitada, GPS, `data_fiscalizacao`, `data_inclusao`, análise e veículo |
| `fiscalizacao_veiculo_imagem` | Obrigatória | Momento de inclusão das fotografias usado na regra de fotografia posterior |
| `movimento_estacionamento_veiculo` | Obrigatória | Períodos adquiridos, valores pagos, pagamento, notificações e movimentações para análise de receita |
| `estacionamento_veiculo` | Obrigatória | Liga os movimentos ao veículo/cliente e fornece as coordenadas da ativação |
| `veiculo_cliente` | Obrigatória | Liga o estacionamento ao veículo e fornece o CPF/login do motorista |
| `veiculo` | Obrigatória | Placa cadastrada e ligação com `veiculo_cliente` |
| `guardador_veiculo_riorotativo_historico` | Obrigatória | Vínculo temporal do CPF do guardador com CNPJ, entidade e situação ativa ou bloqueada |
| `entidade_credenciadora_riorotativo_historico` | Obrigatória | Nome, CNPJ e vigência das entidades |
| `notificacao_veiculo` | Provável | Liga fiscalização, estacionamento e eventual regularização posterior |
| `tipo_periodo` | Condicional | Classificação dos períodos adquiridos |
| `tipo_periodo_tarifa` | Condicional | Reconstrução ou validação do valor tarifário original |
| `tipo_pagamento` | Condicional | Necessária caso o tipo de pagamento afete a receita ou a validação |
| `area_estacionamento_riorotativo` | Condicional | Identificação da área por coordenadas e validação de continuidade na mesma zona |
| `perfil_funcionamento_riorotativo` | Obrigatória | Classificação de vaga inativa fora do dia ou horário de funcionamento |

## Dependências por modelo

### `verificacao_guardador_veiculo`

```text
fiscalizacao_veiculo
├── fiscalizacao_veiculo_imagem
├── veiculo
├── movimento_estacionamento_veiculo
│   └── estacionamento_veiculo
│       └── veiculo_cliente
├── notificacao_veiculo
└── guardador_veiculo_riorotativo_historico
    └── entidade_credenciadora_riorotativo_historico
```

O modelo deve resolver:

- placa efetiva da fiscalização;
- primeira fiscalização válida de cada janela;
- agregação de períodos contínuos;
- fiscalizações sem ativação;
- registros sincronizados posteriormente;
- motivo de não repasse;
- valores arrecadados e de repasse.

### `ordem_pagamento_riorotativo_guardador_veiculo_dia`

Fonte principal:

- `verificacao_guardador_veiculo`.

O histórico do guardador somente precisará ser consultado novamente se o
vínculo, a vigência e a situação do guardador não forem materializados no modelo
granular.

### `ordem_pagamento_riorotativo_entidade_dia`

Fontes:

- `verificacao_guardador_veiculo`;
- `guardador_veiculo_riorotativo_historico`;
- `entidade_credenciadora_riorotativo_historico`.

### Modelo de movimentações/ativações

Fontes mínimas:

- `movimento_estacionamento_veiculo`;
- `estacionamento_veiculo`;
- `veiculo_cliente`;
- `veiculo`.

Fontes adicionais, conforme o grão e as métricas escolhidas:

- `tipo_periodo`;
- `tipo_periodo_tarifa`;
- `tipo_pagamento`;
- `area_estacionamento_riorotativo`.

## Regras e premissas iniciais

### Agregação dos períodos

Períodos consecutivos são primeiro agregados e depois divididos em subperíodos
fixos de duas horas. Cada subperíodo representa R$ 2,00 e pode receber uma
primeira verificação válida de um guardador diferente.

Exemplo:

```text
12h às 14h + 14h às 16h = subperíodos 12h–14h e 14h–16h
```

Períodos com lacuna permanecem como janelas distintas.

### Fechamento

- processamento inicialmente em D+1;
- fiscalizações recebidas após o fechamento não retificam dias anteriores;
- será necessário estabelecer uma regra operacional para registros tardios,
  travados ou sincronizados após a janela de processamento.

### Placa efetiva

Quando houver divergência entre a leitura OCR e a placa digitada, deve
prevalecer a placa digitada:

```sql
coalesce(placa_digitada, placa_ocr)
```

A assertividade do OCR deve ser monitorada separadamente.

### Validação geográfica e de funcionamento

Os motivos de vaga são separados em:

- `VAGA_FORA_VIGENCIA`, quando a área não está vigente no instante da fiscalização;
- `VAGA_FORA_FUNCIONAMENTO`, quando a área está vigente, mas o dia ou horário
  está fora do perfil de funcionamento.

Os dados geográficos da fiscalização podem ser preservados para monitoramento e
auditoria, inclusive para alertas sobre verificações da mesma placa em locais
distintos.

### Isenções

Veículos isentos não geram repasse financeiro ao guardador.

A origem da informação de isenção ainda não está disponível no data lake. Uma
possível integração futura com dados da CET-Rio deverá ser avaliada.

### Bloqueio de guardadores

A regra financeira para divergências entre o bloqueio mantido pela SMTR e o
acesso mantido pela Jaé ainda não está definida.

Inicialmente, essa situação deve ser preservada nos dados para auditoria, sem
criar uma regra definitiva de pagamento retroativo.

## Valores financeiros

### Valores já definidos

| Destinatário | Valor por verificação válida |
| --- | ---: |
| Guardador | R$ 1,40 |
| Cada entidade vinculada | R$ 0,11 |

### Divisão da arrecadação

- a concessionária Jaé retém 4,2% do valor bruto;
- em uma verificação válida, o guardador recebe R$ 1,40 e as duas entidades
  recebem R$ 0,11 cada;
- a PCRJ recebe o residual: valor bruto menos a retenção de 4,2% da Jaé,
  menos R$ 1,40 e menos os dois repasses de R$ 0,11;
- os descontos de R$ 1,40 e R$ 0,11 somente são aplicados a verificações
  válidas.

### Arrecadação bruta e líquida

A especificação deve preservar separadamente:

- valor bruto pago pelo período;
- valor líquido após a retenção da Jaé;
- valor retido pela Jaé.

A concessionária Jaé retém 4,2% do valor bruto. O repasse à PCRJ é o
residual após essa retenção e, para verificações válidas, após os pagamentos
de R$ 1,40 ao guardador e de R$ 0,11 a cada uma das duas entidades.

## Motivos de não repasse

Os cinco motivos oficiais de não repasse são:

| Código | Descrição |
| --- | --- |
| `VAGA_FORA_VIGENCIA` | Área fora da vigência no momento da fiscalização |
| `VAGA_FORA_FUNCIONAMENTO` | Fiscalização fora do dia ou horário do perfil de funcionamento |
| `FOTOGRAFIA_POSTERIOR_PRIMEIRA_VALIDACAO` | Inclusão da fotografia posterior à primeira imagem do subperíodo, usando `data_inclusao` como proxy |
| `AUSENCIA_PERIODO_TARIFADO` | Ausência de período tarifado ativo |
| `OCORRENCIA_DUPLICADA` | Já existe uma verificação anterior no mesmo subperíodo de duas horas |

O modelo mantém somente um motivo, obedecendo à ordem da tabela acima.

## Situação atual no repositório

Já existem capturas para:

- `movimento_estacionamento_veiculo`;
- `estacionamento_veiculo`.

Ainda precisam ser avaliadas ou criadas capturas e declarações de source para:

- `fiscalizacao_veiculo`;
- `fiscalizacao_veiculo_imagem`;
- `veiculo`;
- `veiculo_cliente`;
- `notificacao_veiculo`, se confirmada como necessária.

Os seguintes modelos de cadastro já existem e podem ser reutilizados:

- `guardador_veiculo_riorotativo`;
- `guardador_veiculo_riorotativo_historico`;
- `entidade_credenciadora_riorotativo_historico`;
- `area_estacionamento_riorotativo`;
- `perfil_funcionamento_riorotativo`.

## Definições ainda pendentes

Antes de concluir a implementação, ainda será necessário definir:

1. grão definitivo do modelo de movimentações/ativações;
2. tratamento de verificações recebidas depois do fechamento D+1;
3. regra financeira para guardadores bloqueados em uma base, mas ainda ativos
   em outra;
4. origem futura dos dados de veículos isentos.
