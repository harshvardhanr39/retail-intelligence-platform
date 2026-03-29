FROM apache/airflow:2.8.0

USER airflow

RUN pip install --no-cache-dir \
  apache-airflow-providers-postgres \
  apache-airflow-providers-amazon \
  apache-airflow-providers-slack \
  openlineage-airflow \
  openlineage-python
