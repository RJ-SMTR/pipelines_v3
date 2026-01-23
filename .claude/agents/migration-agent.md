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

## ⚠️ REGRAS IMPORTANTES

### 0. Atualizar Repositório de Origem Antes de Migrar

**SEMPRE** atualize a branch main do repositório de origem antes de iniciar qualquer migração:
```bash
cd /caminho/para/pipelines_rj_smtr
git checkout main
git pull origin main
```

Isso garante que você está migrando a versão mais recente do código.

### 1. Usar Cookiecutter para Criar Novos Pipelines

**SEMPRE** use o cookiecutter para criar a estrutura de um novo pipeline:
```bash
cd /caminho/para/pipelines_v3
uvx cookiecutter templates --output-dir=pipelines
```

Isso garante que todos os arquivos necessários sejam criados de forma padronizada:
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

### 2. Verificar `pipelines/common/` Antes de Criar Código Novo

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
│   ├── secret.py           # get_secret
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

### 3. Copiar Modelos DBT para Flows de Treatment

**Para flows de treatment**, além de migrar o código Python, é necessário copiar os modelos DBT correspondentes ao selector utilizado.

**Estrutura de pastas DBT:**
```
# Repositório origem (1.4)
pipelines_rj_smtr/queries/models/

# Repositório destino (3.0)
pipelines_v3/queries/models/
```

**Passo a passo:**

1. **Identifique o selector** usado no flow de treatment:
```python
   # No arquivo constants.py do flow antigo
   CADASTRO_SELECTOR = DBTSelector(
       name="cadastro",  # <-- Este é o nome do selector
       ...
   )
```

2. **Localize o selector no arquivo de selectors do DBT:**
```bash
   # Repositório origem
   cat pipelines_rj_smtr/queries/selectors.yml
```
   
   Exemplo de selector:
```yaml
   selectors:
     - name: cadastro
       definition:
         method: fqn
         value: "cadastro"  # Pasta/modelo a ser copiado
```

3. **Copie os modelos correspondentes:**
```bash
   # Copiar pasta de modelos
   cp -r pipelines_rj_smtr/queries/models/cadastro/ pipelines_v3/queries/models/
   
   # Copiar schemas (se existirem separadamente)
   cp pipelines_rj_smtr/queries/models/cadastro/schema.yml pipelines_v3/queries/models/cadastro/
```

4. **Copie também os sources relacionados (se necessário):**
```bash
   # Verificar sources usados nos modelos
   grep -r "source(" pipelines_rj_smtr/queries/models/cadastro/
   
   # Copiar definições de sources
   cp pipelines_rj_smtr/queries/models/_sources/cadastro_sources.yml pipelines_v3/queries/models/_sources/
```

5. **Atualize o arquivo `selectors.yml` do repositório destino:**
```bash
   # Adicionar o selector ao arquivo
   vi pipelines_v3/queries/selectors.yml
```

6. **Verifique dependências entre modelos:**
```bash
   # Listar refs usadas nos modelos copiados
   grep -r "ref(" pipelines_v3/queries/models/cadastro/
   
   # Copiar modelos dependentes se necessário
```

**Exemplo completo para migrar `treatment__cadastro`:**
```bash
# 1. Atualizar repo origem
cd /caminho/para/pipelines_rj_smtr
git checkout main && git pull origin main

# 2. Copiar modelos
cp -r queries/models/cadastro/ /caminho/para/pipelines_v3/queries/models/

# 3. Copiar sources relacionados
cp queries/models/_sources/cadastro*.yml /caminho/para/pipelines_v3/queries/models/_sources/

# 4. Verificar e copiar dependências
grep -rh "ref(" /caminho/para/pipelines_v3/queries/models/cadastro/ | sort -u
# Se houver refs para outros modelos, copie-os também

# 5. Atualizar selectors.yml
echo "  - name: cadastro
    definition:
      method: fqn
      value: cadastro" >> /caminho/para/pipelines_v3/queries/selectors.yml
```

---

## Estrutura do Projeto Novo

Cada pipeline é um pacote independente com sua própria estrutura:
```
pipelines/
├── capture__jae_auxiliar/          # Nome: tipo__fonte
│   ├── __init__.py
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
│   ├── cadastro/                   # Modelos do selector cadastro
│   ├── bilhetagem/
│   └── _sources/                   # Definições de sources
├── selectors.yml                   # Definição dos selectors
├── dbt_project.yml
└── profiles.yml
```

---

## Workflow de Migração

### Passo 0: Atualizar repositório de origem
```bash
cd /caminho/para/pipelines_rj_smtr
git checkout main
git pull origin main
```

### Passo 1: Criar estrutura com cookiecutter
```bash
cd /caminho/para/pipelines_v3
uvx cookiecutter templates --output-dir=pipelines
```

Responda as perguntas do template (nome, tipo, etc).

### Passo 2: Verificar o que já existe em common/

Antes de escrever código, cheque:
- Existe task similar em `common/tasks.py`?
- Existe extractor similar em `common/capture/`?
- Existe util similar em `common/utils/`?

### Passo 3: Migrar constants.py

- Remover `Enum`
- Adicionar `tzinfo` nos `datetime()`
- Atualizar imports

### Passo 4: Migrar flow.py

- Usar `@flow` decorator
- Parâmetros viram argumentos da função
- Chamar função genérica (`create_capture_flows_default_tasks` ou `create_materialization_flows_default_tasks`)

### Passo 5: Criar task específica (se necessário)

Se precisar de uma task específica (como `create_jae_general_extractor`):
1. Verificar se pode reutilizar algo de `common/`
2. Se for reutilizável, criar em `common/capture/{fonte}/tasks.py`
3. Se for único, criar no próprio pipeline

### Passo 6: [TREATMENT] Copiar modelos DBT

**Apenas para flows de treatment:**
1. Identificar selector no flow
2. Copiar modelos de `queries/models/{selector}/`
3. Copiar sources relacionados
4. Verificar e copiar dependências (refs)
5. Atualizar `selectors.yml`

### Passo 7: Atualizar prefect.yaml

- Definir schedules (cron)
- Configurar deployments staging e prod

### Passo 8: Atualizar CHANGELOG.md

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
    # chamada direta de função
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
        schedule_cron=create_hourly_cron(),  # cron explícito
        primary_keys=v["primary_keys"],
        # ...
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
        flow_folder_name="capture__jae_auxiliar",  # infere cron do prefect.yaml
        primary_keys=v["primary_keys"],
        # ...
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
    flow_folder_name="treatment__cadastro",  # infere cron do prefect.yaml
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
from pipelines.utils.secret import get_secret
credentials = get_secret(constants.JAE_SECRET_PATH.value)
```

**DEPOIS (3.0):**
```python
from pipelines.common.utils.secret import get_secret
credentials = get_secret(jae_constants.JAE_SECRET_PATH)  # sem .value
```

---

## Padrões de Nomenclatura

| Tipo | Padrão Antigo | Padrão Novo |
|------|---------------|-------------|
| Pasta do flow | `capture/jae/` | `capture__jae_auxiliar/` |
| Nome do flow | `jae: auxiliares - captura` | `capture__jae_auxiliar` |
| Função do flow | `CAPTURA_AUXILIAR` (variável) | `capture__jae_auxiliar` (função) |
| Deployment staging | N/A | `rj-capture--jae_auxiliar--staging` |
| Deployment prod | N/A | `rj-capture--jae_auxiliar--prod` |

---

## Checklist de Migração

Para cada flow:

- [ ] **Atualizar repo origem:** `git checkout main && git pull origin main`
- [ ] Criar estrutura com `uvx cookiecutter templates --output-dir=pipelines`
- [ ] Verificar `common/` para reutilização de código
- [ ] Migrar `constants.py` (remover Enum, usar variáveis diretas)
- [ ] Migrar `flow.py`:
  - [ ] Substituir `with Flow()` por `@flow`
  - [ ] Substituir `Parameter()` por argumentos de função
  - [ ] Usar `runtime` ao invés de `prefect.context`
  - [ ] Substituir `log()` por `print()`
- [ ] Criar tasks específicas em `common/` se reutilizáveis
- [ ] Atualizar imports para `pipelines.common.*`
- [ ] Adicionar `tzinfo` em todos os `datetime()`
- [ ] Remover `.value` das constants
- [ ] **[TREATMENT]** Copiar modelos DBT para `queries/models/`
- [ ] **[TREATMENT]** Copiar sources para `queries/models/_sources/`
- [ ] **[TREATMENT]** Atualizar `queries/selectors.yml`
- [ ] **[TREATMENT]** Verificar dependências (refs) entre modelos
- [ ] Configurar `prefect.yaml` com schedules
- [ ] Atualizar `CHANGELOG.md`
- [ ] Testar localmente

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
from pipelines.common.utils.secret import get_secret
from pipelines.common import constants as smtr_constants
```
