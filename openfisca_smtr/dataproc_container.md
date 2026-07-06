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

## Dockerfile minimo

Criar um diretorio temporario:

```bash
mkdir -p /tmp/dataproc-openfisca-test
cd /tmp/dataproc-openfisca-test
```

Criar `Dockerfile`:

```Dockerfile
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps tini \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir openfisca-core==44.7.0

ENV PYSPARK_PYTHON=/usr/local/bin/python
```

Esta imagem testa apenas `openfisca-core`. Para uso real das regras da SMTR,
tambem sera necessario incluir o pacote `openfisca_smtr` na imagem.

## Build e push

```bash
gcloud builds submit \
  --project="$PROJECT" \
  --region="$REGION" \
  --tag "$REGION-docker.pkg.dev/$PROJECT/$REPOSITORY/$IMAGE_NAME:$IMAGE_VERSION"
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
