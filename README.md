# first setup

docker compose run airflow-webserver airflow db init
docker compose run airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
docker compose up airflow-webserver airflow-scheduler
http://localhost:8080/login/
docker compose down

# continue

docker compose build --no-cache
docker compose up
docker compose down

#

docker compose down --volumes --remove-orphans
