package io.arenadata.dtm.query.execution.core.integration.util;

public class DockerImagesUtil {

    public static final String KAFKA = "confluentinc/cp-kafka";
    public static final String ZOOKEEPER = "confluentinc/cp-zookeeper:4.0.0";
    public static final String DTM_KAFKA_EMULATOR_READER = "ci.arenadata.io/connector-kafka-emulator-reader:latest";
    public static final String DTM_KAFKA_EMULATOR_WRITER = "ci.arenadata.io/db-writer:v3.2.0";
    public static final String DTM_KAFKA_STATUS_MONITOR = "ci.arenadata.io/dtm-status-monitor:latest";
    public static final String DTM_VENDOR_EMULATOR = "ci.arenadata.io/dtm-vendor-emulator:latest";
    public static final String DTM_CORE = "ci.arenadata.io/dtm-core:v3.3.0";
    public static final String ADQM = "yandex/clickhouse-server:latest";
    public static final String ADB = "pivotaldata/gpdb-devel";
    public static final String MARIA_DB = "mariadb:10.5.3";

}
