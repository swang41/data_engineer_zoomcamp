# 1.2.2 ingesting NY Taxi Data to Postgres
```
# setup postgres database
docker run -it `
  -e POSTGRES_USER="root" `
  -e POSTGRES_PASSWORD="root" `
  -e POSTGRES_DB="ny_taxi" `
  -v C:\Users\swang\projects\data_engineer_zoomcamp\week_1\2_DOCKER_SQL\ny_taxi_postgres_data:/var/lib/postgresql/data `
  -p 5432:5432 `
  postgres:13

# connect to the database
pip install pgcli
pgcli -h localhost -p 5432 -u root -d ny_taxi
# run \dt
# run SELECT 1
```

# 1.2.3 Connecing pgAdmin and Postgres
```
docker run -it `
  -e PGADMIN_DEFAULT_EMAIL='admin@admin.com' `
  -e PGADMIN_DEFAULT_PASSWORD='root' `
  -p 8080:80 `
  dpage/pgadmin4

# create network for connect containers
docker network create pg-network

docker run -it `
  -e PGADMIN_DEFAULT_EMAIL='admin@admin.com' `
  -e PGADMIN_DEFAULT_PASSWORD='root' `
  -p 8080:80 `
  --network=pg-network `
  --name pg-admin `
  dpage/pgadmin4

docker run -it `
  -e POSTGRES_USER="root" `
  -e POSTGRES_PASSWORD="root" `
  -e POSTGRES_DB="ny_taxi" `
  -v C:\Users\swang\projects\data_engineer_zoomcamp\week_1\2_DOCKER_SQL\ny_taxi_postgres_data:/var/lib/postgresql/data `
  -p 5432:5432 `
  --network=pg-network `
  --name pg-database1 `
  postgres:13

```

# 1.2.4 Dockerizing the ingestion script

```
$URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest.py `
  --username=root `
  --password=root `
  --host=localhost `
  --port=5432 `
  --db=ny_taxi `
  --table_name=yellow_taxi_trips `
  --url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

docker run -it --network pg-network taxi_ingest:v001 `
  --username=root `
  --password=root `
  --host=pg-database1 `
  --port=5432 `
  --db=ny_taxi `
  --table_name=yellow_taxi_trips `
  --url=${URL}
```