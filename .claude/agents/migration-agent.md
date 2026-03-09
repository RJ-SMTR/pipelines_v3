---
name: migration-agent
description: "Assists with pipeline migration from Prefect 1.4 to Prefect 3.0. Run only when a migration is requested."
model: haiku
---

# Contexto de Migração: Prefect 1.4 → Prefect 3.0

## Repositórios

- **Origem (1.4):** /caminho/para/pipelines_rj_smtr (Prefect 1.4)
- **Destino (3.0):** /caminho/para/pipelines_v3 (Prefect 3.0)

---

## REGRAS OBRIGATÓRIAS

### 0. CRIAR PLANO ANTES DE MIGRAR

**ANTES de qualquer alteração**, você DEVE criar um plano de migração e apresentar ao usuário para validação. Use o EnterPlanMode para isso.

**O plano deve conter:**

1. Nome do flow a ser migrado e tipo (capture/treatment/integration)
2. Arquivos de origem identificados (flow.py, constants.py, tasks.py, utils.py)
3. O que será reutilizado de `pipelines/common/`
4. O que precisará ser criado/adaptado
5. [TREATMENT] Modelos DBT a serem copiados, snapshots, selectors
6. [TREATMENT] Se `schema.yml` já existe no destino e se há diferenças
7. Dependências ou pontos de atenção

**Só prossiga com a implementação após aprovação do usuário.**

### 1. Trabalhar Diretamente no Repositório Destino

**NUNCA** crie arquivos em pasta temporária (`/tmp`, `tmp/`, etc) para depois mover.
**SEMPRE** faça todas as alterações diretamente no repositório destino (`pipelines_v3/`).

### 2. Usar Cookiecutter PRIMEIRO

**SEMPRE** use o cookiecutter como primeiro passo para criar a estrutura do pipeline:

```bash
cd /caminho/para/pipelines_v3
uvx cookiecutter templates --output-dir=pipelines
```

Depois ajuste os arquivos gerados conforme necessário. **Nunca crie a estrutura manualmente.**

Arquivos gerados pelo cookiecutter:

- `pyproject.toml`
- `Dockerfile`
- `prefect.yaml`
- `CHANGELOG.md`
- `constants.py`
- `flow.py`
- `.dockerignore`

**Templates disponíveis:**

- `capture` - Para pipelines de captura de dados
- `treatment` - Para pipelines de materialização/tratamento

### 3. Imports Sempre no Topo do Arquivo

**NUNCA** faça imports dentro de definições de funções (flows, tasks, etc.). Todos os imports necessários devem estar no **topo do arquivo**, após o header e a docstring.

```python
# -*- coding: utf-8 -*-
"""
DBT: 2026-02-04
"""
# CORRETO: imports no topo
from datetime import datetime
from zoneinfo import ZoneInfo
from prefect import flow
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks

@flow(log_prints=True)
def capture__exemplo(env=None, timestamp=None):
    create_capture_flows_default_tasks(...)
```

```python
# ERRADO: imports dentro da função
@flow(log_prints=True)
def capture__exemplo(env=None, timestamp=None):
    from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks  # NÃO FAÇA ISSO
    create_capture_flows_default_tasks(...)
```

### 4. NÃO Criar `__init__.py` nos Flows Migrados

Os pipelines individuais (ex: `capture__jae_auxiliar/`, `treatment__cadastro/`) **NÃO têm** `__init__.py`. Apenas `pipelines/common/` e seus subdiretórios usam `__init__.py`.

**NUNCA** crie `__init__.py` dentro de um pipeline migrado.

### 5. Atualizar Repositório de Origem Antes de Migrar

**SEMPRE** atualize a branch main do repositório de origem antes de iniciar qualquer migração:

```bash
cd /caminho/para/pipelines_rj_smtr
git checkout main
git pull origin main
```

### 6. Verificar `pipelines/common/` Antes de Criar Código Novo

**ANTES** de criar qualquer task ou função nova, **SEMPRE** verifique se já existe algo similar em:

```
pipelines/common/
├── capture/
│   ├── default_capture/    # Flow genérico de captura
│   │   ├── flow.py         # create_capture_flows_default_tasks()
│   │   ├── tasks.py        # Tasks reutilizáveis de captura
│   │   └── utils.py        # SourceCaptureContext, helpers
│   └── jae/                # Específico da Jaé (exemplo)
│       ├── constants.py
│       ├── tasks.py
│       └── utils.py
├── treatment/
│   └── default_treatment/  # Flow genérico de materialização
│       ├── flow.py         # create_materialization_flows_default_tasks()
│       ├── tasks.py        # Tasks reutilizáveis de tratamento
│       └── utils.py        # DBTSelector, DBTTest, helpers
├── utils/
│   ├── gcp/
│   │   ├── bigquery.py     # SourceTable, Dataset, BQTable
│   │   └── storage.py      # Storage
│   ├── extractors/
│   │   └── db.py           # get_raw_db, get_raw_db_paginated
│   ├── database.py         # create_database_url, test_database_connection
│   ├── discord.py          # send_discord_message
│   ├── fs.py               # save_local_file, read_raw_data
│   ├── pretreatment.py     # transform_to_nested_structure
│   ├── redis.py            # get_redis_client
│   ├── secret.py           # get_env_secret
│   └── utils.py            # convert_timezone, cron helpers
├── constants.py            # Constantes globais (TIMEZONE, PROJECT_NAME, etc)
└── tasks.py                # Tasks genéricas (get_run_env, get_scheduled_timestamp)
```

**Ordem de verificação:**

1. `pipelines/common/tasks.py` - Tasks genéricas
2. `pipelines/common/capture/default_capture/` - Para flows de captura
3. `pipelines/common/treatment/default_treatment/` - Para flows de tratamento
4. `pipelines/common/utils/` - Funções utilitárias
5. `pipelines/common/capture/{fonte}/` - Código específico da fonte (ex: jae)

**Se encontrar algo similar:** Reutilize ou estenda
**Se não encontrar:** Crie em `common/` se for reutilizável, ou no pipeline específico se for único

### 7. [TREATMENT] Copiar Modelos DBT - Verificar Antes de Sobrescrever

**Para flows de treatment**, além de migrar o código Python, copie os modelos DBT correspondentes.

**IMPORTANTE - Verificação de `schema.yml`:**
Antes de copiar, verifique se `schema.yml` já existe no repositório destino:

```bash
# Verificar se já existe
ls pipelines_v3/queries/models/{dominio}/schema.yml
```

- **Se NÃO existe**: copie normalmente
- **Se existe**: compare os arquivos. Se forem iguais, não é necessário copiar. Se forem diferentes, **PARE e informe ao usuário** as diferenças para que ele decida o que fazer. **Nunca sobrescreva automaticamente.**

**Estrutura de pastas DBT:**

```
# Repositório origem (1.4)
pipelines_rj_smtr/queries/
├── models/
│   └── {dominio}/
│       ├── staging/
│       │   ├── stg_*.sql
│       │   └── aux_*.sql
│       ├── *.sql
│       ├── schema.yml
│       └── CHANGELOG.md
└── snapshots/
    └── {dominio}/
        └── snapshot_*.sql

# Repositório destino (3.0) - mesma estrutura
pipelines_v3/queries/
├── models/
│   └── {dominio}/
└── snapshots/
    └── {dominio}/
```

**Passo a passo:**

1. **Identifique o selector** usado no flow de treatment (em `constants.py`)
2. **Localize o selector** no arquivo `selectors.yml` da origem
3. **Verifique se já existem arquivos no destino** antes de copiar:
   ```bash
   ls -la pipelines_v3/queries/models/{dominio}/ 2>/dev/null
   ls -la pipelines_v3/queries/snapshots/{dominio}/ 2>/dev/null
   ```
4. **Copie a pasta completa de modelos** (se não existir no destino):
   ```bash
   cp -r pipelines_rj_smtr/queries/models/{dominio}/ pipelines_v3/queries/models/
   ```
5. **Copie os snapshots** (se existirem e não estiverem no destino):
   ```bash
   mkdir -p pipelines_v3/queries/snapshots/{dominio}/
   cp pipelines_rj_smtr/queries/snapshots/{dominio}/*.sql pipelines_v3/queries/snapshots/{dominio}/
   ```
6. **Verifique sources, selectors e dependências**:
   ```bash
   # Sources usados
   grep -roh "source('[^']*', '[^']*')" pipelines_v3/queries/models/{dominio}/ | sort -u
   # Selectors no destino
   grep "name: {selector}" pipelines_v3/queries/selectors.yml
   # Refs entre modelos
   grep -roh "ref('[^']*')" pipelines_v3/queries/models/{dominio}/ | sort -u
   ```
7. **Teste a compilação:**
   ```bash
   cd pipelines_v3/queries
   dbt parse --select {dominio}
   ```

### 8. Docstrings: Apenas no Início do Arquivo flow.py

**NÃO** adicione docstring na definição da função do flow (no `@flow` ou `def`).
Adicione docstring **apenas no início do arquivo** `flow.py`, após o header:

```python
# -*- coding: utf-8 -*-
"""
DBT: 2026-02-04
"""
from prefect import flow
...

@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__exemplo(env=None, ...):  # SEM docstring aqui
    create_materialization_flows_default_tasks(...)
```

### 9. CHANGELOG Simples

Mantenha o CHANGELOG simples, seguindo o padrão dos outros CHANGELOGs do repositório. Use um placeholder para o link do PR:

```markdown
# Changelog - {flow_type}\_\_{pipeline_name}

## [1.0.0] - YYYY-MM-DD

### Adicionado

- Migração do flow `{nome_flow_antigo}` do Prefect 1.4 para Prefect 3.0 (https://github.com/RJ-SMTR/pipelines_v3/pull/XXX)
```

**Não invente versões além da 1.0.0 para migrações iniciais. Use a data atual.**

### 10. NÃO Criar Arquivos de Documentação Extras

**NUNCA** crie múltiplos arquivos de documentação, guias de migração, READMEs, ou arquivos de suporte durante a migração. Exemplos do que **NÃO** fazer:

- `MIGRATION_GUIDE.md`
- `MIGRATION_NOTES.md`
- `README.md` dentro do pipeline
- `SUMMARY.md`
- Qualquer outro `.md` além do `CHANGELOG.md`

Ao final da migração, apresente **apenas um resumo direto na conversa** com as informações principais:

- Arquivos criados/modificados
- Decisões tomadas
- Pontos de atenção ou ações pendentes

---

## Estrutura do Projeto Novo

Cada pipeline é um pacote independente com sua própria estrutura:

```
pipelines/
├── capture__jae_auxiliar/          # Nome: tipo__fonte
│   ├── CHANGELOG.md
│   ├── Dockerfile
│   ├── constants.py
│   ├── flow.py
│   ├── prefect.yaml
│   └── pyproject.toml
├── treatment__cadastro/
│   ├── ...
├── common/                          # Biblioteca compartilhada
│   └── ...
└── templates/                       # Templates do cookiecutter
    ├── capture/
    └── treatment/

queries/                             # Modelos DBT
├── models/
│   ├── cadastro/
│   ├── bilhetagem/
│   └── _sources/
├── selectors.yml
├── dbt_project.yml
└── profiles.yml
```

**Nota:** Pipelines individuais NÃO têm `__init__.py`. Apenas `common/` e seus subdiretórios.

---

## Workflow de Migração

### Passo 0: Criar plano e validar com o usuário

1. Ler o flow de origem (flow.py, constants.py, tasks.py, utils.py)
2. Verificar o que existe em `common/` que pode ser reutilizado
3. [TREATMENT] Verificar modelos DBT, snapshots, schema.yml no destino
4. Apresentar plano detalhado ao usuário via EnterPlanMode
5. **Aguardar aprovação antes de prosseguir**

### Passo 1: Atualizar repositório de origem

```bash
cd /caminho/para/pipelines_rj_smtr
git checkout main
git pull origin main
```

### Passo 2: Criar estrutura com cookiecutter

```bash
cd /caminho/para/pipelines_v3
uvx cookiecutter templates --output-dir=pipelines
```

Responda as perguntas do template (nome, tipo, etc).

### Passo 3: Ajustar os arquivos gerados

#### constants.py

- Remover `Enum`
- Adicionar `tzinfo` nos `datetime()`
- Atualizar imports para `pipelines.common.*`
- Remover `.value` das constants

#### flow.py

- Usar `@flow` decorator (gerado pelo cookiecutter)
- Parâmetros viram argumentos da função
- Chamar função genérica (`create_capture_flows_default_tasks` ou `create_materialization_flows_default_tasks`)
- Docstring apenas no início do arquivo, NÃO na função do flow

### Passo 4: Criar tasks específicas (se necessário)

1. Verificar se pode reutilizar algo de `common/`
2. Se for reutilizável, criar em `common/capture/{fonte}/tasks.py`
3. Se for único, criar no próprio pipeline

### Passo 5: [TREATMENT] Copiar modelos DBT

1. Verificar se modelos/schema.yml já existem no destino
2. Se schema.yml existir e for diferente, **perguntar ao usuário**
3. Copiar modelos, snapshots, verificar sources e selectors
4. Testar com `dbt parse --select {dominio}`

### Passo 6: Atualizar prefect.yaml

- Definir schedules (cron)
- Configurar deployments staging e prod

### Passo 7: Atualizar CHANGELOG.md

- Formato simples, versão 1.0.0, placeholder para PR

### Passo 8: Resumo final

- Apresentar resumo na conversa (NÃO criar arquivo)
- Listar arquivos criados/modificados
- Indicar ações pendentes

---

## Mapeamento de Conceitos

### 1. Definição de Flow

**ANTES (1.4):**

```python
from prefect import Parameter, case, unmapped
from prefeitura_rio.pipelines_utils.custom import Flow

with Flow("jae: auxiliares - captura") as CAPTURA_AUXILIAR:
    table_id = Parameter(name="table_id", default="linha")
    timestamp = Parameter(name="timestamp", default=None)
    recapture = Parameter(name="recapture", default=False)

    # tasks...
```

**DEPOIS (3.0):**

```python
# -*- coding: utf-8 -*-
"""
Common 2026-02-11
"""
from prefect import flow
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run

@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__jae_auxiliar(
    env=None,
    source_table_ids=tuple([s.table_id for s in sources]),
    timestamp=None,
    recapture=True,
    recapture_days=2,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(...)
```

### 2. Tasks

**ANTES (1.4):**

```python
from prefect import task
from pipelines.constants import constants

@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def minha_task(param):
    log("mensagem")  # from prefeitura_rio.pipelines_utils.logging import log
    return resultado
```

**DEPOIS (3.0):**

```python
from prefect import task

@task
def minha_task(param):
    print("mensagem")  # usa print() com log_prints=True no flow
    return resultado
```

### 3. Constants

**ANTES (1.4):**

```python
from enum import Enum

class constants(Enum):
    JAE_SOURCE_NAME = "jae"
    JAE_SECRET_PATH = "smtr_jae_access_data"

# Uso: constants.JAE_SOURCE_NAME.value
```

**DEPOIS (3.0):**

```python
# Variáveis diretas (sem Enum)
JAE_SOURCE_NAME = "jae"
JAE_SECRET_PATH = "smtr_jae_access_data"

# Uso: JAE_SOURCE_NAME
```

### 4. SourceTable

**ANTES (1.4):**

```python
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.schedules import create_hourly_cron

JAE_AUXILIAR_SOURCES = [
    SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=k,
        first_timestamp=datetime(2024, 1, 7, 0, 0, 0),
        schedule_cron=create_hourly_cron(),
        primary_keys=v["primary_keys"],
    )
]
```

**DEPOIS (3.0):**

```python
from pipelines.common.utils.gcp.bigquery import SourceTable

JAE_AUXILIAR_SOURCES = [
    SourceTable(
        source_name=jae_constants.JAE_SOURCE_NAME,
        table_id=k,
        first_timestamp=v.get(
            "first_timestamp",
            datetime(2024, 1, 7, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        ),
        flow_folder_name="capture__jae_auxiliar",
        primary_keys=v["primary_keys"],
    )
]
```

### 5. DBTSelector

**ANTES (1.4):**

```python
from pipelines.treatment.templates.utils import DBTSelector
from pipelines.schedules import create_hourly_cron

CADASTRO_SELECTOR = DBTSelector(
    name="cadastro",
    schedule_cron=create_hourly_cron(minute=10),
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0),
)
```

**DEPOIS (3.0):**

```python
from pipelines.common.treatment.default_treatment.utils import DBTSelector

CADASTRO_SELECTOR = DBTSelector(
    name="cadastro",
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__cadastro",
)
```

### 6. Runtime e Context

**ANTES (1.4):**

```python
import prefect

flow_name = prefect.context.flow_name
flow_run_id = prefect.context.flow_run_id
```

**DEPOIS (3.0):**

```python
from prefect import runtime

flow_name = runtime.flow_run.flow_name
deployment_name = runtime.deployment.name
scheduled_start_time = runtime.flow_run.scheduled_start_time
task_run_id = runtime.task_run.id
```

### 7. Ambiente (dev/prod)

**ANTES (1.4):**

```python
# Inferido pelo agent label ou variável de ambiente
```

**DEPOIS (3.0):**

```python
from prefect import runtime

def get_run_env(env, deployment_name):
    if deployment_name is not None:
        env = "prod" if deployment_name.endswith("--prod") else "dev"
    # ...
```

### 8. Logging

**ANTES (1.4):**

```python
from prefeitura_rio.pipelines_utils.logging import log
log("mensagem", level="info")
```

**DEPOIS (3.0):**

```python
print("mensagem")  # com log_prints=True no decorator @flow
```

### 9. Secrets

**ANTES (1.4):**

```python
from pipelines.utils.secret import get_env_secret
credentials = get_env_secret(constants.JAE_SECRET_PATH.value)
```

**DEPOIS (3.0):**

```python
from pipelines.common.utils.secret import get_env_secret
credentials = get_env_secret(jae_constants.JAE_SECRET_PATH)  # sem .value
```

---

## Padrões de Nomenclatura

| Tipo               | Padrão Antigo                 | Padrão Novo                         |
| ------------------ | ----------------------------- | ----------------------------------- |
| Pasta do flow      | `capture/jae/`                | `capture__jae_auxiliar/`            |
| Nome do flow       | `jae: auxiliares - captura`   | `capture__jae_auxiliar`             |
| Função do flow     | `CAPTURA_AUXILIAR` (variável) | `capture__jae_auxiliar` (função)    |
| Deployment staging | N/A                           | `rj-capture--jae_auxiliar--staging` |
| Deployment prod    | N/A                           | `rj-capture--jae_auxiliar--prod`    |

---

## Checklist de Migração

Para cada flow:

- [ ] **Plano criado e aprovado pelo usuário**
- [ ] **Atualizar repo origem:** `git checkout main && git pull origin main`
- [ ] Criar estrutura com `uvx cookiecutter templates --output-dir=pipelines`
- [ ] Verificar `common/` para reutilização de código
- [ ] Ajustar `constants.py` (remover Enum, usar variáveis diretas, adicionar tzinfo)
- [ ] Ajustar `flow.py`:
  - [ ] Docstring apenas no início do arquivo (não na função)
  - [ ] Substituir `with Flow()` por `@flow`
  - [ ] Substituir `Parameter()` por argumentos de função
  - [ ] Usar `runtime` ao invés de `prefect.context`
  - [ ] Substituir `log()` por `print()`
- [ ] Criar tasks específicas em `common/` se reutilizáveis
- [ ] Atualizar imports para `pipelines.common.*`
- [ ] Remover `.value` das constants
- [ ] **NÃO** criar `__init__.py` no pipeline
- [ ] **[TREATMENT]** Verificar se modelos/schema.yml já existem no destino
- [ ] **[TREATMENT]** Se schema.yml diferente, perguntar ao usuário
- [ ] **[TREATMENT]** Copiar modelos DBT (staging/, schema.yml, CHANGELOG.md)
- [ ] **[TREATMENT]** Copiar snapshots (se existirem)
- [ ] **[TREATMENT]** Verificar sources em `sources.yml`
- [ ] **[TREATMENT]** Verificar selectors em `selectors.yml`
- [ ] **[TREATMENT]** Verificar dependências (refs) entre modelos
- [ ] **[TREATMENT]** Testar compilação com `dbt parse --select {dominio}`
- [ ] Configurar `prefect.yaml` com schedules
- [ ] Atualizar `CHANGELOG.md` (formato simples, placeholder para PR)
- [ ] **NÃO** criar arquivos de documentação extras
- [ ] Apresentar resumo final na conversa

---

## Imports Comuns

```python
# Flow e runtime
from prefect import flow, runtime, unmapped
from prefect.tasks import Task

# Tasks comuns (VERIFICAR ANTES DE CRIAR NOVAS)
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    setup_environment,
)

# Captura
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run

# Tratamento
from pipelines.common.treatment.default_treatment.flow import create_materialization_flows_default_tasks
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run, DBTSelector

# Utils (VERIFICAR ANTES DE CRIAR NOVAS)
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.secret import get_env_secret
from pipelines.common import constants as smtr_constants
```
