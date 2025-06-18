FROM apache/airflow:3.0.2
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY intersystems_irispython-3.2.0-py3-none-any.whl .
RUN pip install intersystems_irispython-3.2.0-py3-none-any.whl
