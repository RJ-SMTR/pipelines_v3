# Modelo relacional do Rio Rotativo para Miro

O bloco Mermaid abaixo pode ser colado no aplicativo **Diagramas Mermaid** de
um board da Miro. Depois da pré-visualização, use **Adicionar ao board**.

```mermaid
erDiagram
    VEICULO o|--o{ FISCALIZACAO_VEICULO : identifica
    FISCALIZACAO_VEICULO ||--o{ FISCALIZACAO_VEICULO_IMAGEM : possui_imagens
    VEICULO ||--o{ VEICULO_CLIENTE : possui_vinculo
    VEICULO_CLIENTE ||--o{ ESTACIONAMENTO_VEICULO : realiza_ativacao
    ESTACIONAMENTO_VEICULO ||--o{ MOVIMENTO_ESTACIONAMENTO_VEICULO : possui_movimento
    FISCALIZACAO_VEICULO ||--o{ NOTIFICACAO_VEICULO : origina
    ESTACIONAMENTO_VEICULO ||--o{ NOTIFICACAO_VEICULO : regulariza
    NOTIFICACAO_VEICULO o|--o{ MOVIMENTO_ESTACIONAMENTO_VEICULO : associa

    AREA_ESTACIONAMENTO_RIOROTATIVO ||--o{ MOVIMENTO_ESTACIONAMENTO_RIOROTATIVO : localiza
    MOVIMENTO_ESTACIONAMENTO_VEICULO ||--|| MOVIMENTO_ESTACIONAMENTO_RIOROTATIVO : transforma
    ESTACIONAMENTO_VEICULO ||--o{ MOVIMENTO_ESTACIONAMENTO_RIOROTATIVO : enriquece
    VEICULO_CLIENTE ||--o{ MOVIMENTO_ESTACIONAMENTO_RIOROTATIVO : identifica_motorista
    VEICULO ||--o{ MOVIMENTO_ESTACIONAMENTO_RIOROTATIVO : identifica_placa

    FISCALIZACAO_VEICULO ||--|| VERIFICACAO_GUARDADOR_VEICULO : consolida
    MOVIMENTO_ESTACIONAMENTO_RIOROTATIVO o|--o{ VERIFICACAO_GUARDADOR_VEICULO : forma_janela_tarifada
    GUARDADOR_VEICULO_HISTORICO o|--o{ VERIFICACAO_GUARDADOR_VEICULO : identifica_guardador
    ENTIDADE_CREDENCIADORA_HISTORICO ||--o{ GUARDADOR_VEICULO_HISTORICO : credencia

    VERIFICACAO_GUARDADOR_VEICULO ||--o{ ORDEM_PAGAMENTO_GUARDADOR_DIA : agrega_por_guardador_dia
    VERIFICACAO_GUARDADOR_VEICULO ||--o{ ORDEM_PAGAMENTO_ENTIDADE_DIA : agrega_por_entidade_dia
    ENTIDADE_CREDENCIADORA_HISTORICO ||--o{ ORDEM_PAGAMENTO_ENTIDADE_DIA : identifica_beneficiaria

    FISCALIZACAO_VEICULO {
        int id PK
        int id_status_fiscalizacao_veiculo FK
        string tx_login
        float latitude
        float longitude
        string placa_ocr
        string placa_digitada
        int id_veiculo FK
        datetime data_fiscalizacao
        datetime data_analise
        datetime data_inclusao
    }

    FISCALIZACAO_VEICULO_IMAGEM {
        int id_fiscalizacao_veiculo FK
        bytes imagem_placa
        bytes imagem1_veiculo
        bytes imagem2_veiculo
        datetime data_inclusao
    }

    VEICULO {
        int id PK
        string placa UK
        int id_modelo_veiculo
        string cor
        datetime data_inclusao
    }

    VEICULO_CLIENTE {
        int id PK
        string tx_login
        int id_veiculo FK
        datetime data_inativacao
        datetime data_inclusao
    }

    ESTACIONAMENTO_VEICULO {
        int id PK
        int id_veiculo_cliente FK
        float latitude
        float longitude
        datetime data_inclusao
    }

    MOVIMENTO_ESTACIONAMENTO_VEICULO {
        int id PK
        int id_estacionamento_veiculo FK
        int id_tipo_periodo FK
        numeric valor_periodo
        datetime data_periodo_inicial
        datetime data_periodo_final
        int id_tipo_pagamento FK
        datetime data_pagamento
        numeric valor_pago
        int id_notificacao_veiculo FK
        string uuid_movimento
        datetime data_inclusao
    }

    NOTIFICACAO_VEICULO {
        int id PK
        int id_fiscalizacao_veiculo FK
        int id_estacionamento_veiculo FK
        datetime data_inclusao
    }

    AREA_ESTACIONAMENTO_RIOROTATIVO {
        string id_area PK
        string nome
        geography geometry
        int quantidade_vaga_total
        date data_inicio_vigencia
        date data_fim_vigencia
    }

    GUARDADOR_VEICULO_HISTORICO {
        string id_guardador_historico PK
        date data
        string documento
        string cnpj FK
        string status
    }

    ENTIDADE_CREDENCIADORA_HISTORICO {
        string cnpj PK
        date data_inicio_vigencia PK
        date data_fim_vigencia
        string razao_social
        string nome_fantasia
    }

    MOVIMENTO_ESTACIONAMENTO_RIOROTATIVO {
        date data PK
        int id_movimento PK
        int id_estacionamento FK
        int id_veiculo
        string placa_veiculo
        string cpf_motorista
        string id_area FK
        datetime datetime_periodo_inicial
        datetime datetime_periodo_final
        numeric valor_pago_bruto
        numeric valor_repasse_concessionaria
        numeric valor_repasse_pcrj
    }

    VERIFICACAO_GUARDADOR_VEICULO {
        date data PK
        string id_verificacao PK
        datetime datetime_verificacao
        string cpf_guardador
        string cpf_motorista
        string placa_veiculo
        boolean indicador_verificacao_valida
        string motivo_nao_repasse
        numeric valor_pago_bruto
        numeric valor_repasse_concessionaria
        numeric valor_repasse_guardador
        numeric valor_repasse_entidade_a
        numeric valor_repasse_entidade_b
        numeric valor_repasse_pcrj
    }

    ORDEM_PAGAMENTO_GUARDADOR_DIA {
        date data_ordem PK
        string cpf_guardador PK
        int quantidade_verificacao_total
        int quantidade_verificacao_valida
        int quantidade_verificacao_invalida
        numeric valor_repasse_guardador
    }

    ORDEM_PAGAMENTO_ENTIDADE_DIA {
        date data_ordem PK
        string cnpj PK
        string nome_entidade
        int quantidade_verificacao_valida
        int quantidade_guardador_ativo
        numeric valor_repasse_entidade
    }
```

## Rateio financeiro da verificação válida

```text
Concessionária Jaé = valor bruto × 4,2%
Guardador          = R$ 1,40
Entidade A         = R$ 0,11
Entidade B         = R$ 0,11
PCRJ               = valor bruto - Jaé - R$ 1,40 - R$ 0,11 - R$ 0,11
```

Os valores de guardador e entidades só são descontados quando a verificação
é válida.

Os períodos pagos contínuos são divididos em subperíodos de duas horas e
R$ 2,00. A primeira fiscalização de cada subperíodo pode gerar repasse; as
seguintes são ocorrências duplicadas. `data_inclusao` da imagem é usada como
proxy do momento da fotografia.

## Motivos oficiais de não repasse

- `VAGA_FORA_VIGENCIA`
- `VAGA_FORA_FUNCIONAMENTO`
- `AUSENCIA_PERIODO_TARIFADO`
- `OCORRENCIA_DUPLICADA`
- `FOTOGRAFIA_POSTERIOR_PRIMEIRA_VALIDACAO`

Somente um motivo é mantido, na ordem apresentada acima.

DDL source_jae:
CREATE TABLE public.fiscalizacao_veiculo ( id serial4 NOT NULL, id_status_fiscalizacao_veiculo int2 NOT NULL, tx_login varchar(128) NOT NULL, latitude float8 NOT NULL, longitude float8 NOT NULL, placa_ocr varchar(10) NOT NULL, placa_digitada varchar(10) NULL, id_veiculo int4 NULL, data_fiscalizacao timestamp NOT NULL, data_analise timestamp NULL, data_inclusao timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL, CONSTRAINT fiscalizacao_veiculo_pkey PRIMARY KEY (id), CONSTRAINT fkfiscalizac225649 FOREIGN KEY (id_veiculo) REFERENCES public.veiculo(id), CONSTRAINT fkfiscalizac245140 FOREIGN KEY (id_status_fiscalizacao_veiculo) REFERENCES public.status_fiscalizacao_veiculo(id) );

CREATE TABLE public.veiculo ( id serial4 NOT NULL, placa varchar(10) NOT NULL, id_modelo_veiculo int2 NOT NULL, cor varchar(40) NULL, data_inclusao timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL, CONSTRAINT veiculo_pkey PRIMARY KEY (id), CONSTRAINT veiculo_placa_key UNIQUE (placa), CONSTRAINT fkveiculo321299 FOREIGN KEY (id_modelo_veiculo) REFERENCES public.modelo_veiculo(id) );

CREATE TABLE public.veiculo_cliente ( id serial4 NOT NULL, tx_login varchar(128) NOT NULL, id_veiculo int4 NOT NULL, data_inativacao timestamp NULL, data_inclusao timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL, CONSTRAINT veiculo_cliente_pkey PRIMARY KEY (id), CONSTRAINT fkveiculo_cl734880 FOREIGN KEY (id_veiculo) REFERENCES public.veiculo(id) );

CREATE TABLE public.estacionamento_veiculo ( id bigserial NOT NULL, id_veiculo_cliente int4 NOT NULL, latitude float8 NOT NULL, longitude float8 NOT NULL, data_inclusao timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL, CONSTRAINT estacionamento_veiculo_pkey PRIMARY KEY (id), CONSTRAINT fkestacionam268328 FOREIGN KEY (id_veiculo_cliente) REFERENCES public.veiculo_cliente(id) );

CREATE TABLE public.movimento_estacionamento_veiculo ( id bigserial NOT NULL, id_estacionamento_veiculo int8 NOT NULL, id_tipo_periodo int2 NOT NULL, valor_periodo numeric(10, 2) NOT NULL, data_periodo_inicial timestamp NOT NULL, data_periodo_final timestamp NULL, id_tipo_pagamento int2 NOT NULL, data_pagamento timestamp NOT NULL, valor_pago numeric(10, 2) NOT NULL, id_notificacao_veiculo int4 NULL, uuid_movimento_estacionamento_veiculo uuid DEFAULT uuid_generate_v4() NOT NULL, data_inclusao timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL, CONSTRAINT movimento_estacionamento_veiculo_pkey PRIMARY KEY (id), CONSTRAINT fkmovimento_566591 FOREIGN KEY (id_tipo_pagamento) REFERENCES public.tipo_pagamento(id), CONSTRAINT fkmovimento_648492 FOREIGN KEY (id_tipo_periodo) REFERENCES public.tipo_periodo(id), CONSTRAINT fkmovimento_796459 FOREIGN KEY (id_estacionamento_veiculo) REFERENCES public.estacionamento_veiculo(id), CONSTRAINT fkmovimento_97658 FOREIGN KEY (id_notificacao_veiculo) REFERENCES public.notificacao_veiculo(id) );

CREATE TABLE public.notificacao_veiculo (
	id serial4 NOT NULL,
	id_fiscalizacao_veiculo int4 NOT NULL,
	id_estacionamento_veiculo int8 NULL,
	data_inclusao timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT notificacao_veiculo_pkey PRIMARY KEY (id)
);
-- public.notificacao_veiculo foreign keys
ALTER TABLE public.notificacao_veiculo ADD CONSTRAINT fknotificaca773114 FOREIGN KEY (id_fiscalizacao_veiculo) REFERENCES public.fiscalizacao_veiculo(id);
ALTER TABLE public.notificacao_veiculo ADD CONSTRAINT fknotificaca842191 FOREIGN KEY (id_estacionamento_veiculo) REFERENCES public.estacionamento_veiculo(id);

CREATE TABLE public.fiscalizacao_veiculo_imagem (
	id_fiscalizacao_veiculo int4 NOT NULL,
	imagem_placa bytea NOT NULL,
	imagem1_veiculo bytea NOT NULL,
	imagem2_veiculo bytea NOT NULL,
	data_inclusao timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT fiscalizacao_veiculo_imagem_pkey PRIMARY KEY (id_fiscalizacao_veiculo)
);
-- public.fiscalizacao_veiculo_imagem foreign keys
ALTER TABLE public.fiscalizacao_veiculo_imagem ADD CONSTRAINT fkfiscalizac626786 FOREIGN KEY (id_fiscalizacao_veiculo) REFERENCES public.fiscalizacao_veiculo(id);