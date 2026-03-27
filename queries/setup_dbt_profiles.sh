mkdir -p ./credentials-dev
mkdir -p ./credentials-prod
mkdir -p ./profiles

echo "Mounting files from env..."

echo "$1" > ./credentials-dev/dev.json
echo "$1" > ./credentials-prod/prod.json

echo """
queries:
  target: prod
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: rj-smtr
      dataset: dbt
      location: US
      threads: 2
      keyfile: $PWD/credentials-dev/dev.json
    prod:
      type: bigquery
      method: service-account
      project: rj-smtr
      dataset: dbt
      location: US
      threads: 2
      keyfile: $PWD/credentials-prod/prod.json""" > profiles/profiles.yml

cat profiles/profiles.yml