package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

import java.util.List;

public interface KafkaMppwSqlFactory extends MppwSqlFactory {

    String createKeyColumnsSqlQuery(String schema, String tableName);

    List<ColumnMetadata> createKeyColumnQueryMetadata();

    String moveOffsetsExtTableSqlQuery(String schema, String table);

    String commitOffsetsSqlQuery(String schema, String table);

    String insertIntoKadbOffsetsSqlQuery(String schema, String table);

    String createExtTableSqlQuery(String server, List<String> columnNameTypeList, MppwKafkaRequest request,
                                  MppwProperties mppwProperties);

    String checkServerSqlQuery(String database, String brokerList);

    String createServerSqlQuery(String database, String brokerList);

    List<String> getColumnsFromEntity(Entity entity);

    String dropExtTableSqlQuery(String schema, String table);

    String insertIntoStagingTableSqlQuery(String schema, String columns, String table, String extTable);

    String getTableName(String requestId);

    String getServerName(String database);
}
