# Imagem Dataproc para OpenFisca

Este guia descreve como criar e publicar uma imagem Docker minima para testar
OpenFisca em modelos dbt Python executados no Dataproc Serverless.

## Objetivo

O modelo dbt Python `teste_openfisca` confirmou que:

- o dbt consegue submeter o modelo Python ao Dataproc Serverless;
- o container padrao do Dataproc nao possui `openfisca-core`;
- o runtime remoto nao conseguiu instalar `openfisca-core` via `pip install`
  durante a execucao do job.

Portanto, para usar OpenFisca nesse caminho, a dependencia precisa estar
disponivel antes da execucao do batch, por exemplo em uma imagem customizada.

## Runtime e versao do Python (importante)

**Usar o runtime default do dbt-bigquery (1.1) e casar o Python da imagem com
ele (3.10).** Nao definir `runtime_config.version` no `profiles.yml`.

O codigo Python do Spark e montado no container pelo runtime do Dataproc
Serverless e executado pelo `PYSPARK_PYTHON` da imagem — as versoes precisam
ser compativeis:

| Runtime | Spark | Python do runtime | Python maximo na imagem |
|---|---|---|---|
| 1.1 (default do dbt) | 3.3.4 | 3.10 | **3.10** (cloudpickle do Spark 3.3 nao suporta 3.11+) |
| 2.2 | 3.5.1 | 3.12 | 3.12 (3.13 removeu `typing.io`; 3.14 segfaulta) |

Falhas observadas com Python errado (diagnostico de 2026-07-09):

- Python 3.11/3.12 no Spark 3.3 → `PicklingError: tuple index out of range`
  (cloudpickle) — no batch aparece como falha generica sem traceback;
- Python 3.13 no Spark 3.3 → `ModuleNotFoundError: No module named 'typing.io'`;
- Python 3.14 no Spark 3.5 → **segfault** na serializacao, falha generica.

**O runtime 2.2 esta descartado neste ambiente**: qualquer modelo dbt Python
(mesmo trivial, sem OpenFisca, no container padrao) falha na escrita do
BigQuery com erro generico. Causa desconhecida — requer logs (infra). Nota: o
runtime 1.1 saiu de suporte em 2025-07-31 (disponivel ate 2027-07),
entao em algum momento a causa no 2.2+ precisara ser investigada.

Alem disso, o numpy precisa ser **< 2**: o `pyspark.pandas` dos Sparks atuais
usa `np.NaN` (removido no numpy 2.0). Na imagem minima atual isso e apenas
protecao (sem pandas o wrapper do dbt nem importa `pyspark.pandas`), mas o pin
em `requirements.txt` evita a regressao se pandas entrar na imagem.

## Depuracao sem acesso aos logs

Falha generica (`Google Cloud Dataproc Agent reports job failure`) significa
que o driver morreu sem exceçao Python limpa. Exceçoes limpas chegam ate o dbt
via `stateMessage` do batch — entao um "modelo-sonda" que executa cada etapa
em `try/except` e levanta `raise Exception(relatorio)` no final da
visibilidade completa do ambiente sem precisar de logs. O modelo
`teste_dataproc_minimo` serve de controle: se ele falha, o problema e de
ambiente; se passa e o modelo real falha, o problema e do modelo.

Caso ja conhecido: escrever por cima de uma tabela criada por outro caminho
(ex.: escrita manual local via connector) pode falhar de forma generica —
dropar a tabela resolve.

## Connector BigQuery

O dbt-bigquery submete por default o jar
`spark-bigquery-with-dependencies_2.13-0.34.0.jar` junto ao batch. No runtime
1.1 (Scala 2.12, connector 0.28.1 embutido) esse jar Scala 2.13 e ignorado na
pratica e o embutido e usado — funciona. Nao e necessario configurar
`jar_file_uri` no modelo enquanto o runtime for o default.

## Permissoes necessarias

Para criar e enviar a imagem, a conta de quem executa o comando precisa conseguir:

- criar builds no Cloud Build (`cloudbuild.builds.create`);
- listar ou criar repositorios no Artifact Registry;
- enviar artefatos Docker para o repositorio (`artifactregistry.repositories.uploadArtifacts`).

O service account usado pelo Cloud Build tambem precisa ter permissao de escrita
no repositorio Artifact Registry, por exemplo `Artifact Registry Writer`.

## Variaveis

```bash
PROJECT=rj-smtr-dev
REGION=us-central1
REPOSITORY=dataproc
IMAGE_NAME=openfisca-smtr
IMAGE_VERSION=test
```

## Checar conta e projeto

```bash
gcloud auth list --filter=status:ACTIVE
gcloud config get-value project
```

## Verificar ou criar o repositorio Docker

Listar repositorios existentes:

```bash
gcloud artifacts repositories list \
  --project="$PROJECT" \
  --location="$REGION"
```

Criar o repositorio, se necessario:

```bash
gcloud artifacts repositories create "$REPOSITORY" \
  --project="$PROJECT" \
  --location="$REGION" \
  --repository-format=docker \
  --description="Dataproc custom containers"
```

## Dockerfile

O `Dockerfile` versionado neste diretorio (`openfisca_smtr/Dockerfile`) segue o
template oficial de container customizado do Dataproc Serverless (Debian 12 +
Miniforge + pacotes da imagem padrao), com Python pinado em 3.12 e
`openfisca-core` instalado via `requirements.txt`.

A imagem inclui apenas `openfisca-core`. Para uso real das regras da SMTR,
tambem sera necessario incluir o pacote `openfisca_smtr` na imagem.

## Build e push

Com Cloud Build (requer a API habilitada no projeto):

```bash
gcloud builds submit openfisca_smtr/ \
  --project="$PROJECT" \
  --region="$REGION" \
  --tag "$REGION-docker.pkg.dev/$PROJECT/$REPOSITORY/$IMAGE_NAME:$IMAGE_VERSION"
```

Ou com Docker local (caminho usado ate agora — a API do Cloud Build nao esta
habilitada no `rj-smtr`):

```bash
gcloud auth print-access-token | docker login -u oauth2accesstoken \
  --password-stdin "https://$REGION-docker.pkg.dev"

docker build -t "$REGION-docker.pkg.dev/$PROJECT/$REPOSITORY/$IMAGE_NAME:$IMAGE_VERSION" openfisca_smtr/
docker push "$REGION-docker.pkg.dev/$PROJECT/$REPOSITORY/$IMAGE_NAME:$IMAGE_VERSION"
```

Se este comando concluir com sucesso, a conta tem permissao suficiente para
buildar e enviar a imagem.

## Usar no dbt Python

Depois que a imagem existir, configurar o Dataproc Serverless no
`queries/profiles.yml`:

```yaml
runtime_config:
  container_image: us-central1-docker.pkg.dev/rj-smtr-dev/dataproc/openfisca-smtr:test
  properties:
    spark.executor.instances: "2"
    spark.driver.memory: 4g
    spark.driver.memoryOverhead: 1g
```

Depois rodar o teste:

```bash
cd queries
dbt build --select teste_openfisca --profiles-dir ./dev
```

## Observacoes

- A imagem atual dos jobs Prefect fica em `ghcr.io/rj-smtr/...`; o Dataproc
  Serverless espera imagens em Container Registry ou Artifact Registry.
- Reaproveitar diretamente a imagem Prefect nao e o melhor primeiro passo,
  porque ela carrega dependencias de orquestracao e nao foi desenhada como
  imagem customizada do Dataproc.
- O caminho recomendado e uma imagem especifica para Dataproc, pequena e com
  apenas as dependencias Python necessarias para os modelos dbt Python.
