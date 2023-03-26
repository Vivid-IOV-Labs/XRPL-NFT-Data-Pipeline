FROM apache/airflow:2.5.2-python3.9
COPY requirements.txt /requirements.txt
COPY sls_lambda /opt/airflow/dag/sls_lambda
COPY utilities /opt/airflow/dag/utilities
COPY .env /opt/airflow/.env
RUN pip install --no-cache-dir --user -r /requirements.txt