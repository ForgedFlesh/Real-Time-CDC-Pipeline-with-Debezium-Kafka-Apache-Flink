FROM flink:1.20.3-scala_2.12

ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG JDBC_CONNECTOR_VERSION=3.3.0-1.20
ARG POSTGRES_DRIVER_VERSION=42.7.7

RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
 && wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar \
 && wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar
