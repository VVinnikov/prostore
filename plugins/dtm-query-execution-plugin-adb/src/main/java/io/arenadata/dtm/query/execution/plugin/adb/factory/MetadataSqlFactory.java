package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;

import java.util.List;

/**
 * Factory for creating DDL scripts based on metadata
 */
public interface MetadataSqlFactory {

    String createDropTableScript(Entity entity);

    String createTableScripts(Entity entity);

    String createSchemaSqlQuery(String schemaName);

    String dropSchemaSqlQuery(String schemaName);

    String createKeyColumnsSqlQuery(String schema, String tableName);

    String createSecondaryIndexSqlQuery(String schema, String tableName);

    List<ColumnMetadata> createKeyColumnQueryMetadata();

    String createWritableExtTableSqlQuery(String schema, String table, List<String> columns, String topic, List<String> brokerList, Integer chunkSize);

    String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql);

    String dropWritableExtTableSqlQuery(String schema, String table);
}
