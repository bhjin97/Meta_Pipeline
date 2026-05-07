FROM apache/airflow:2.10.5-python3.8

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk wget procps && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ✅ 수정: OCI ARM 서버 환경에 맞게 amd64 → arm64 변경
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow

RUN pip install --no-cache-dir \
    # ✅ 추가: Airflow 버전 명시적으로 고정
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-apache-spark" \
    "pyspark==3.5.1"