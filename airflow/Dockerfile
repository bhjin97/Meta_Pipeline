FROM apache/airflow:2.10.5-python3.8

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk wget procps && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# OCI ARM 서버 환경에 맞게 amd64 → arm64 변경
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Spark 실행에 필요한 JAR 사전 포함 (dag에 포함 시 병렬 설치로 충돌 일어남 )
RUN mkdir -p /opt/airflow/jars && \
    wget -O /opt/airflow/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -O /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -O /opt/airflow/jars/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

USER airflow

RUN pip install --no-cache-dir \
    # Airflow 버전 명시적으로 고정
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-apache-spark" \
    "pyspark==3.5.1"  \
    "psycopg2-binary" \
    "docker"