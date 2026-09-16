# Загрузка плагина в отдельном этапе: runtime Connect не содержит пакетного менеджера.
FROM registry.access.redhat.com/ubi9/ubi:9.8 AS plugin-builder
RUN dnf install -y curl-minimal tar gzip && dnf clean all
# Штатный healthcheck Connect вызывает curl, которого в образе нет, ставим и потом перенесем его.
RUN dnf install -y --installroot=/curl-root --releasever=9 \
    --setopt=install_weak_deps=False --nodocs curl-minimal
ARG DEBEZIUM_VERSION=3.5.2.Final
ARG JMX_EXPORTER_VERSION=1.5.0
RUN curl -fsSL --retry 3 \
    "https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar" \
    -o /tmp/jmx_prometheus_javaagent.jar
RUN mkdir -p /plugins && curl -fsSL --retry 3 \
    "https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
    -o /tmp/debezium.tar.gz && tar -xzf /tmp/debezium.tar.gz -C /plugins

FROM confluentinc/cp-kafka-connect:8.3.1
COPY --from=plugin-builder /curl-root/usr/ /usr/
COPY --from=plugin-builder --chown=appuser:appuser /plugins/ /usr/share/confluent-hub-components/
COPY --from=plugin-builder /tmp/jmx_prometheus_javaagent.jar /opt/jmx_prometheus_javaagent.jar
COPY kafka-connect.yml /opt/kafka-connect.yml
USER appuser
