package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;

public interface KafkaMpprSqlFactory extends  MpprSqlFactory {

    String createWritableExtTableSqlQuery(MpprKafkaRequest request);

    String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql);

    String dropWritableExtTableSqlQuery(String schema, String table);

    String getTableName(String requestId);
}
