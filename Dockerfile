FROM apache/airflow:3.0.2
COPY requirements.txt .
RUN pip="$(pip install -r requirements.txt)" && echo $pip
RUN pip install intersystems-irispython
