package ru.ibs.dtm.query.execution.plugin.adb.factory;

import ru.ibs.dtm.common.model.ddl.ClassTable;

/**
 * Factory for creating DDL scripts based on metadata
 */
public interface MetadataSqlFactory {

    String createDropTableScript(ClassTable classTable);

    String createTableScripts(ClassTable classTable);

    String createSchemaSqlQuery(String schemaName);

    String dropSchemaSqlQuery(String schemaName);

    String createKeyColumnsSqlQuery(String schema, String tableName);
}
