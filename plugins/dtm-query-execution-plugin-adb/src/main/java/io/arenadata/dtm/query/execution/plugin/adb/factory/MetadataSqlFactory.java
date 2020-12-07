package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

import java.util.List;

/**
 * Factory for creating DDL scripts based on metadata
 */
public interface MetadataSqlFactory {

    String createDropTableScript(Entity entity);

    String createSchemaSqlQuery(String schemaName);

    String dropSchemaSqlQuery(String schemaName);

    String createKeyColumnsSqlQuery(String schema, String tableName);

    String createSecondaryIndexSqlQuery(String schema, String tableName);

    List<ColumnMetadata> createKeyColumnQueryMetadata();

    String createWritableExtTableSqlQuery(MpprRequest request);

    String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql);

    String dropWritableExtTableSqlQuery(String schema, String table);

    String dropExtTableSqlQuery(String schema, String table);

    String createExtTableSqlQuery(String server, List<String> columnNameTypeList, MppwRequestContext context, MppwProperties mppwProperties);

    String checkServerSqlQuery(String database, String brokerList);

    String createServerSqlQuery(String database, String brokerList);

    String insertIntoStagingTableSqlQuery(String schema, String columns, String table, String extTable);

    List<String> getColumnsFromEntity(Entity entity);
}