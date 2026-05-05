FROM apache/spark:3.5.1

USER root

RUN mkdir -p /opt/spark/.ivy2 /tmp/mc-config \
 && chown -R spark:spark /opt/spark/.ivy2 /tmp/mc-config

USER spark