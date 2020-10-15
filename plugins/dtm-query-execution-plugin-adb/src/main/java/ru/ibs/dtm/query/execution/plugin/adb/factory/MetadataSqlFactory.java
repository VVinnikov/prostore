package ru.ibs.dtm.query.execution.plugin.adb.factory;

import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
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

    List<ColumnMetadata> createKeyColumnQueryMetadata();
}
