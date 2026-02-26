# 📋 Dependências a Migrar - treatment__viagem_informada

**Status**: 🚀 **ROTEIRO DE MIGRAÇÃO DAS DEPENDÊNCIAS**

Este documento lista tudo que precisa ser migrado para que `treatment__viagem_informada` funcione completamente.

---

## 📊 Resumo Executivo

O flow `treatment__viagem_informada` depende de:

| Tipo | Nome | Origem | Status | Prioridade |
|------|------|--------|--------|-----------|
| Flow Prefect | treatment__planejamento | pipelines_rj_smtr | ❌ Falta | 🔴 CRÍTICA |
| Flow Prefect | capture__rioonibus | pipelines_rj_smtr | ❌ Falta | 🔴 CRÍTICA |
| Modelo DBT | calendario | pipelines_rj_smtr | ❌ Falta | 🔴 CRÍTICA |
| Modelo DBT | aux_calendario_manual | pipelines_rj_smtr | ❌ Falta | 🔴 CRÍTICA |
| Modelo DBT | routes_gtfs | pipelines_rj_smtr | ✅ Existe | 🟢 OK |
| Código comum | pipelines/common | pipelines_rj_smtr | ✅ Existe | 🟢 OK |

---

## 🔴 CRÍTICO - Flows Prefect a Migrar

### 1. treatment__planejamento

**Origem**: `/home/botelho/prefeitura_rio/pipelines_rj_smtr/pipelines/treatment/planejamento/`

**O que migrar**:
```
treatment/planejamento/
├── flow.py              # Flow principal
├── constants.py         # Constantes e seletores DBT
├── prefect.yaml         # Deployments
├── pyproject.toml       # Dependências
├── Dockerfile           # Docker image
├── __init__.py
└── utils/               # Utilitários (se houver)
```

**Destino em pipelines_v3**:
```
pipelines_v3/pipelines/treatment__planejamento/
```

**Razão da dependência**:
- `PLANEJAMENTO_DIARIO_SELECTOR` - Usado em `constants.py` de `treatment__viagem_informada`
- O flow aguarda conclusão de `planejamento` antes de executar

**Nota**: Será um novo diretório em pipelines_v3 com padrão cookiecutter

---

### 2. capture__rioonibus

**Origem**: `/home/botelho/prefeitura_rio/pipelines_rj_smtr/pipelines/capture/rioonibus/`

**O que migrar**:
```
capture/rioonibus/
├── flow.py              # Flow principal
├── constants.py         # Constantes e sources
├── prefect.yaml         # Deployments
├── pyproject.toml       # Dependências
├── Dockerfile           # Docker image
├── __init__.py
└── utils/               # Utilitários (se houver)
```

**Destino em pipelines_v3**:
```
pipelines_v3/pipelines/capture__rioonibus/
```

**Razão da dependência**:
- `VIAGEM_INFORMADA_SOURCE` - Usado em `constants.py` de `treatment__viagem_informada`
- O flow aguarda dados da Rio Ônibus antes de executar transformação
- Source de dados primária: `source_rioonibus.viagem_informada`

**Nota**: Será um novo diretório em pipelines_v3 com padrão cookiecutter

---

## 🔴 CRÍTICO - Modelos DBT a Migrar

### 1. calendario (Modelo Principal)

**Origem**: `/home/botelho/prefeitura_rio/pipelines_rj_smtr/queries/models/planejamento/calendario.sql`

**Local de destino**:
```
pipelines_v3/queries/models/planejamento/calendario.sql
```

**Tamanho**: ~200 linhas

**Usado por**:
- `viagem_informada_monitoramento.sql` (linha 193)
- Enriquecimento com dados de calendário (feeds GTFS)

**Também migrar**:
- Schema documentation em `planejamento/schema.yml`

---

### 2. aux_calendario_manual (Model Auxiliar)

**Origem**: `/home/botelho/prefeitura_rio/pipelines_rj_smtr/queries/models/planejamento/staging/aux_calendario_manual.sql`

**Local de destino**:
```
pipelines_v3/queries/models/planejamento/staging/aux_calendario_manual.sql
```

**Tamanho**: ~50 linhas

**Usado por**:
- `calendario.sql` (dependência)
- Dados auxiliares de calendário manual

**Também migrar**:
- Schema documentation em `planejamento/staging/schema.yml`

---

## 🟢 JÁ EXISTE - Models DBT

### routes_gtfs

**Status**: ✅ **Já existe em pipelines_v3**

```
pipelines_v3/queries/models/gtfs/routes_gtfs.sql
pipelines_v3/queries/models/cadastro/staging/aux_routes_vigencia_gtfs.sql
```

**Nada a fazer**: Modelo já copiado

---

## 📋 Checklist de Migração

### Fase 1: Flows Prefect
- [ ] Migrar `treatment__planejamento` (novo pipeline)
  - [ ] Criar diretório `pipelines_v3/pipelines/treatment__planejamento/`
  - [ ] Copiar arquivos de `pipelines_rj_smtr/pipelines/treatment/planejamento/`
  - [ ] Seguir padrão cookiecutter
  - [ ] Criar PR para review

- [ ] Migrar `capture__rioonibus` (novo pipeline)
  - [ ] Criar diretório `pipelines_v3/pipelines/capture__rioonibus/`
  - [ ] Copiar arquivos de `pipelines_rj_smtr/pipelines/capture/rioonibus/`
  - [ ] Seguir padrão cookiecutter
  - [ ] Criar PR para review

### Fase 2: Modelos DBT
- [ ] Migrar `calendario.sql` e schema
  - [ ] Copiar: `pipelines_rj_smtr/queries/models/planejamento/calendario.sql`
  - [ ] Para: `pipelines_v3/queries/models/planejamento/calendario.sql`
  - [ ] Merge schema em `planejamento/schema.yml`

- [ ] Migrar `aux_calendario_manual.sql` e schema
  - [ ] Copiar: `pipelines_rj_smtr/queries/models/planejamento/staging/aux_calendario_manual.sql`
  - [ ] Para: `pipelines_v3/queries/models/planejamento/staging/aux_calendario_manual.sql`
  - [ ] Merge schema em `planejamento/staging/schema.yml`

- [ ] Validar `routes_gtfs`
  - [ ] ✅ Já existe em pipelines_v3
  - [ ] Nada a fazer

### Fase 3: Validação Completa
- [ ] Todos os flows migrados
- [ ] Todos os models DBT copiados
- [ ] Schemas mergeados
- [ ] dbt parse executa sem erros
- [ ] Testes locais Prefect passam
- [ ] Docker build funciona

---

## 🔗 Dependências Entre Componentes

```
treatment__viagem_informada (treatment__planejamento) (capture__rioonibus)
        ↓                                  ↓                    ↓
        └──────────────────┬──────────────┴────────────────────┘
                           ↓
        viagem_informada_monitoramento.sql
        ├── calendario (dependency)
        ├── routes_gtfs (dependency)
        ├── staging_viagem_informada_rioonibus (source)
        └── staging_viagem_informada_brt (source)
```

---

## 📋 Ordem Recomendada de Migração

### 1. PRIMEIRA: capture__rioonibus
- **Razão**: É uma captura, base de tudo
- **Tempo**: 2-3 horas
- **Complexidade**: Média

### 2. SEGUNDA: treatment__planejamento
- **Razão**: Dependência crítica
- **Tempo**: 2-3 horas
- **Complexidade**: Média-Alta

### 3. TERCEIRA: Modelos DBT (calendario + aux_calendario_manual)
- **Razão**: Dependência dos models
- **Tempo**: 30 minutos
- **Complexidade**: Baixa (cópia de arquivos)

### 4. QUARTA: Validação end-to-end
- **Razão**: Garantir que tudo funciona junto
- **Tempo**: 1 hora
- **Complexidade**: Média

---

## ⏱️ Tempo Total Estimado

| Fase | Tempo |
|------|-------|
| Migrar capture__rioonibus | 2-3h |
| Migrar treatment__planejamento | 2-3h |
| Migrar modelos DBT | 30min |
| Testes e validação | 1h |
| **TOTAL** | **6-8 horas** |

---

## 📍 Localizações de Referência

### Em pipelines_rj_smtr
```
/home/botelho/prefeitura_rio/pipelines_rj_smtr/
├── pipelines/
│   ├── treatment/planejamento/     ← treatment__planejamento
│   ├── capture/rioonibus/          ← capture__rioonibus
│   └── common/                     ✅ Já copiado
│
└── queries/
    └── models/
        ├── planejamento/
        │   ├── calendario.sql                    ← Copiar
        │   └── staging/aux_calendario_manual.sql ← Copiar
        └── gtfs/routes_gtfs.sql                  ✅ Já existe
```

### Em pipelines_v3 (Destino)
```
/home/botelho/prefeitura_rio/pipelines_v3/
├── pipelines/
│   ├── treatment__viagem_informada/  ✅ Já existe (migração atual)
│   ├── treatment__planejamento/      ❌ Falta (próxima migração)
│   ├── capture__rioonibus/           ❌ Falta (próxima migração)
│   └── common/                       ✅ Já existe
│
└── queries/
    └── models/
        ├── monitoramento/            ✅ Models de viagem_informada
        ├── planejamento/             ❌ Falta calendario + aux
        └── gtfs/routes_gtfs.sql      ✅ Já existe
```

---

## 🎯 Próximas PRs Necessárias

### PR 1: Migração de capture__rioonibus
- **Título**: `Feat: Migra capture__rioonibus de Prefect 1.4 para 3.0`
- **Arquivos**: Novo pipeline em `pipelines/capture__rioonibus/`
- **Modelos DBT**: Se houver

### PR 2: Migração de treatment__planejamento
- **Título**: `Feat: Migra treatment__planejamento de Prefect 1.4 para 3.0`
- **Arquivos**: Novo pipeline em `pipelines/treatment__planejamento/`
- **Modelos DBT**: Novos models em `queries/models/planejamento/`

### PR 3: Validação end-to-end de treatment__viagem_informada
- **Título**: `Test: Valida treatment__viagem_informada com todas as dependências`
- **Arquivos**: Possíveis ajustes menores
- **Testes**: Deploy e execução em staging

---

## 📝 Notas Importantes

1. **Padrão Cookiecutter**: Ambos os flows devem seguir o padrão do cookiecutter
2. **Schema Files**: Não esquecer de migrar os schema.yml junto com os models DBT
3. **Sources em dbt_project.yml**: Verificar se `source_rioonibus` está configurada corretamente
4. **Timezone**: Todos os models usam `'America/Sao_Paulo'` - manter consistência
5. **Incremental Logic**: O modelo calendario é bastante complexo - testar bem em staging

---

## ✅ Resultado Final

Quando TODAS as dependências estiverem migradas:

✅ `capture__rioonibus` - Capturando dados de viagens
↓
✅ `treatment__planejamento` - Gerando calendário
↓
✅ `treatment__viagem_informada` - Materializando viagens com enriquecimento
↓
✅ Flow pronto para produção!

---

**Documento criado**: 26 de fevereiro de 2026
**Status**: 📋 Roteiro de migração das dependências
