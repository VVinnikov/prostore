package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

import java.util.List;

/**
 * Factory for creating DDL scripts based on metadata
 */
public interface MetadataSqlFactory {

    String createDropTableScript(String tableNameWithSchema);

    String createSchemaSqlQuery(String schemaName);

    String dropSchemaSqlQuery(String schemaName);

    String createKeyColumnsSqlQuery(String schema, String tableName);

    String createSecondaryIndexSqlQuery(String schema, String tableName);

    List<ColumnMetadata> createKeyColumnQueryMetadata();

    String createWritableExtTableSqlQuery(MpprKafkaRequest request);

    String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql);

    String dropWritableExtTableSqlQuery(String schema, String table);

    String dropExtTableSqlQuery(String schema, String table);

    String moveOffsetsExtTableSqlQuery(String schema, String table);

    String insertIntoKadbOffsetsSqlQuery(String schema, String table);

    String createExtTableSqlQuery(String server, List<String> columnNameTypeList, MppwKafkaRequest request,
                                  MppwProperties mppwProperties);

    String checkServerSqlQuery(String database, String brokerList);

    String createServerSqlQuery(String database, String brokerList);

    String insertIntoStagingTableSqlQuery(String schema, String columns, String table, String extTable);

    List<String> getColumnsFromEntity(Entity entity);
}
