FROM debezium/connect:2.6.2.Final

ARG JMX_EXPORTER_VERSION=0.20.0
USER root
RUN mkdir -p /kafka/jmx && \
    curl --retry 5 --retry-delay 3 -fsSL -o /kafka/jmx/jmx_prometheus_javaagent.jar \
    https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar && \
    chmod 755 /kafka/jmx && chmod 644 /kafka/jmx/jmx_prometheus_javaagent.jar
USER kafka
