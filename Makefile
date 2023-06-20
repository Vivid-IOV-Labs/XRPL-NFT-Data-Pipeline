docker-start:
	cp -r sls_lambda utilities dags
	docker build . --tag pkt_afw_ext:latest
	docker compose up airflow-init
	docker compose up -d

docker-stop:
	docker compose down -v

deploy-dev:
	mv psycopg2-2 psycopg2
	sls deploy --stage dev
	mv psycopg2 psycopg2-2

trigger-setup:
	mv psycopg2 psycopg2-new
	python3.9 trigger.py price-trigger
	python3.9 trigger.py volume-trigger
	mv psycopg2-new psycopg2

amount-update:
	mv psycopg2 psycopg2-new
	python3.9 amount_update.py
	mv psycopg2-new psycopg2

load-nixer:
	mv psycopg2 psycopg2-new
	python3.9 nixer-offer-loader.py
	mv psycopg2-new psycopg2

