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

**Para flows de treatment**, além de migrar o código Python, é necessário copiar os modelos DBT correspondentes ao selector utilizado, incluindo modelos de staging, snapshots e documentação.

**Estrutura de pastas DBT:**
```
# Repositório origem (1.4)
pipelines_rj_smtr/queries/
├── models/
│   └── {dominio}/              # Ex: cadastro, transito, bilhetagem
│       ├── staging/            # Modelos de staging (transformações intermediárias)
│       │   ├── stg_*.sql       # Modelos prefixados com stg_
│       │   └── aux_*.sql       # Modelos auxiliares
│       ├── *.sql               # Modelos finais
│       ├── schema.yml          # Documentação e testes dos modelos
│       └── CHANGELOG.md        # Histórico de mudanças
└── snapshots/
    └── {dominio}/
        └── snapshot_*.sql      # Snapshots para rastreamento histórico

# Repositório destino (3.0)
pipelines_v3/queries/
├── models/
│   └── {dominio}/              # Mesma estrutura
└── snapshots/
    └── {dominio}/              # Mesma estrutura
```

**Passo a passo:**

1. **Identifique o selector** usado no flow de treatment:
```python
   # No arquivo constants.py do flow antigo
   CADASTRO_SELECTOR = DBTSelector(
       name="cadastro",  # <-- Este é o nome do selector
       ...
   )

   # Se houver snapshot, também estará aqui:
   SNAPSHOT_CADASTRO_SELECTOR = DBTSelector(
       name="snapshot_cadastro",
       ...
   )
```

2. **Localize o selector no arquivo de selectors do DBT:**
```bash
   # Repositório origem
   cat pipelines_rj_smtr/queries/selectors.yml | grep -A 10 "name: cadastro"
```

   Exemplo de selector:
```yaml
   selectors:
     - name: cadastro
       definition:
         method: fqn
         value: "cadastro"  # Pasta/modelo a ser copiado

     - name: snapshot_cadastro
       definition:
         method: fqn
         value: "snapshot_cadastro"  # Snapshot também pode ter selector
```

3. **Copie a pasta completa de modelos:**
```bash
   # Copiar pasta inteira de modelos (inclui staging/, schema.yml, CHANGELOG.md, etc)
   cp -r pipelines_rj_smtr/queries/models/cadastro/ pipelines_v3/queries/models/
```

   Isso inclui automaticamente:
   - Modelos finais (*.sql)
   - Modelos de staging (staging/*.sql)
   - Modelos auxiliares (aux_*.sql)
   - Documentação (schema.yml) com testes e descrições
   - Histórico (CHANGELOG.md)

4. **Copie os snapshots relacionados (se existirem):**
```bash
   # Verificar se há snapshots para este domínio
   ls -la pipelines_rj_smtr/queries/snapshots/{dominio}/

   # Copiar snapshots (crie o diretório se não existir)
   mkdir -p pipelines_v3/queries/snapshots/{dominio}/
   cp pipelines_rj_smtr/queries/snapshots/{dominio}/*.sql pipelines_v3/queries/snapshots/{dominio}/

   # Exemplo com transito:
   mkdir -p pipelines_v3/queries/snapshots/transito/
   cp pipelines_rj_smtr/queries/snapshots/transito/*.sql pipelines_v3/queries/snapshots/transito/
```

5. **Verifique se os sources estão definidos:**
```bash
   # Identificar sources usados nos modelos
   grep -roh "source('[^']*', '[^']*')" pipelines_v3/queries/models/cadastro/ | sort -u

   # Verificar se estão em pipelines_v3/queries/models/sources.yml
   grep "name: {source_name}" pipelines_v3/queries/models/sources.yml

   # Se faltar, copiar do repositório origem
   # Isso geralmente já está no destino para os domínios principais
```

6. **Verifique o arquivo `selectors.yml` do repositório destino:**
```bash
   # Verificar se o selector já existe
   grep "name: cadastro" pipelines_v3/queries/selectors.yml

   # Se não existir, copiar a definição completa do selector
   # do arquivo de origem para o de destino
```

7. **Verifique dependências entre modelos:**
```bash
   # Listar refs usadas nos modelos copiados
   grep -roh "ref('[^']*')" pipelines_v3/queries/models/cadastro/ | sort -u

   # Se houver refs para modelos em outros domínios, verificar se já existem
   # Se não existirem, copiar também esses modelos dependentes
```

**Exemplo completo para migrar `treatment__transito_autuacao`:**
```bash
# 1. Atualizar repo origem
cd /caminho/para/pipelines_rj_smtr
git checkout main && git pull origin main

# 2. Copiar modelos (inclui staging/, schema.yml, CHANGELOG.md)
cp -r queries/models/transito/ /caminho/para/pipelines_v3/queries/models/

# 3. Copiar snapshots (se existirem)
mkdir -p /caminho/para/pipelines_v3/queries/snapshots/transito/
cp queries/snapshots/transito/*.sql /caminho/para/pipelines_v3/queries/snapshots/transito/

# 4. Verificar sources usados
grep -roh "source('[^']*', '[^']*')" /caminho/para/pipelines_v3/queries/models/transito/ | sort -u
# Resultado: source('transito_staging', 'infracoes_renainf'), etc

# 5. Verificar se sources existem no destino
grep -A 5 "name: transito_staging" /caminho/para/pipelines_v3/queries/models/sources.yml

# 6. Verificar selectors no destino
grep -A 5 "name: transito_autuacao" /caminho/para/pipelines_v3/queries/selectors.yml
grep -A 5 "name: snapshot_transito" /caminho/para/pipelines_v3/queries/selectors.yml

# 7. Verificar refs entre modelos
grep -roh "ref('[^']*')" /caminho/para/pipelines_v3/queries/models/transito/ | sort -u
# Se houver refs para modelos em outros domínios, copiar também

# 8. Testar compilação dos modelos
cd /caminho/para/pipelines_v3/queries
dbt parse --select transito  # Verifica se os modelos estão corretos
```

---

## 🎯 Observações Importantes da Prática

### DBT Models - O que Copiar

Ao copiar modelos DBT, **sempre copie a pasta inteira** do domínio, não apenas arquivos individuais:

```bash
# ✓ CORRETO: Copia tudo de uma vez
cp -r pipelines_rj_smtr/queries/models/transito/ pipelines_v3/queries/models/

# ✗ ERRADO: Copia apenas alguns arquivos
cp pipelines_rj_smtr/queries/models/transito/*.sql pipelines_v3/queries/models/transito/
```

**Por quê?** Porque a pasta inclui:
- Subdiretórios (staging/, etc)
- schema.yml (testes e documentação)
- CHANGELOG.md (histórico)
- Todos os arquivos SQL na estrutura correta

### Snapshots

Snapshots são **frequentemente esquecidos** mas essenciais para rastreamento histórico:

```bash
# Sempre verificar e copiar snapshots
ls pipelines_rj_smtr/queries/snapshots/
# Se existir um diretório com o domínio, copiar
mkdir -p pipelines_v3/queries/snapshots/transito/
cp pipelines_rj_smtr/queries/snapshots/transito/*.sql pipelines_v3/queries/snapshots/transito/
```

### Sources, Selectors e Testes

**Antes de criar um novo código Python**, sempre verifique:

1. **Sources.yml:** Geralmente os sources já existem no destino para domínios principais
   ```bash
   grep -A 5 "name: transito_staging" pipelines_v3/queries/models/sources.yml
   ```

2. **Selectors.yml:** Pode ser que o selector já exista
   ```bash
   grep "name: transito_autuacao" pipelines_v3/queries/selectors.yml
   ```

3. **Schema.yml:** Contém testes importantes que já vêm com a cópia dos modelos
   ```bash
   grep -A 10 "tests:" pipelines_v3/queries/models/transito/schema.yml
   ```

### Validação Final

Sempre teste a compilação após copiar:
```bash
cd pipelines_v3/queries
dbt parse --select transito  # Valida se tudo está correto
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
2. Copiar modelos de `queries/models/{dominio}/` **com toda a estrutura** (staging/, schema.yml, CHANGELOG.md)
3. Copiar snapshots de `queries/snapshots/{dominio}/` (se existirem)
4. Verificar se sources estão definidos em `sources.yml`
5. Verificar se selectors já existem em `selectors.yml`
6. Verificar e copiar dependências (refs) entre modelos
7. Testar compilação com `dbt parse --select {dominio}`

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
- [ ] **[TREATMENT]** Copiar modelos DBT para `queries/models/` (inclui staging/, schema.yml, CHANGELOG.md)
- [ ] **[TREATMENT]** Copiar snapshots para `queries/snapshots/{dominio}/` (se existirem)
- [ ] **[TREATMENT]** Verificar se sources estão definidos em `queries/models/sources.yml`
- [ ] **[TREATMENT]** Verificar se selectors existem em `queries/selectors.yml`
- [ ] **[TREATMENT]** Verificar dependências (refs) entre modelos e copiar modelos dependentes se necessário
- [ ] **[TREATMENT]** Testar compilação com `dbt parse --select {dominio}`
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
