FROM apache/airflow:2.8.0-python3.11

USER root

# Java é obrigatório para o PySpark funcionar dentro do Airflow
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jdk \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/airflow
