docker-start:
	cp -r sls_lambda utilities dags
	docker build . --tag pkt_afw_ext:latest
	docker compose up airflow-init
	docker compose up -d

docker-stop:
	docker compose down -v

deploy-dev:
	sls deploy --stage dev

trigger-setup:
	mv psycopg2 psycopg2-new
	python trigger.py
	mv psycopg2-new psycopg2

