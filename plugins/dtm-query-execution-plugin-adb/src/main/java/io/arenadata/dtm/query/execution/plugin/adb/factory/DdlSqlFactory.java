package io.arenadata.dtm.query.execution.plugin.adb.factory;

public interface DdlSqlFactory {

    String createSchemaSqlQuery(String schemaName);

    String dropSchemaSqlQuery(String schemaName);

    String createDropTableScript(String tableNameWithSchema);

    String createSecondaryIndexSqlQuery(String schema, String tableName);
}
