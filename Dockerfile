FROM apache/airflow:2.5.2-python3.9
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt
COPY sls_lambda /opt/airflow/dags/sls_lambda
COPY utilities /opt/airflow/dags/utilities
COPY .env /opt/airflow/.env