package io.arenadata.dtm.query.execution.core.ddl.service;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.metadata.service.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class QueryResultDdlExecutor implements DdlExecutor<QueryResult> {
    protected final MetadataExecutor<DdlRequestContext> metadataExecutor;
    protected final ServiceDbFacade serviceDbFacade;

    protected QueryRequest replaceDatabaseInSql(QueryRequest request) {
        String sql = request.getSql().replaceAll("(?i) database", " schema");
        request.setSql(sql);
        return request;
    }

    protected String getSchemaName(String requestDatamart, String sqlNodeName) {
        int indexComma = sqlNodeName.indexOf(".");
        return indexComma == -1 ? requestDatamart : sqlNodeName.substring(0, indexComma);
    }

    protected String getTableName(String sqlNodeName) {
        int indexComma = sqlNodeName.indexOf(".");
        return sqlNodeName.substring(indexComma + 1);
    }

    protected String getTableNameWithSchema(String schema, String tableName) {
        return schema + "." + tableName;
    }
}
