docker-start:
	cp -r sls_lambda utilities dags
	docker build . --tag pkt_afw_ext:latest
	docker compose up airflow-init
	docker compose up -d

docker-stop:
	docker compose down -v
