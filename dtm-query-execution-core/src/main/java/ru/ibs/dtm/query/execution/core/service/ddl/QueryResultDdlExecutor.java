package ru.ibs.dtm.query.execution.core.service.ddl;

import lombok.AllArgsConstructor;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;

@AllArgsConstructor
public abstract class QueryResultDdlExecutor implements DdlExecutor<QueryResult> {
    protected final MetadataExecutor<DdlRequestContext> metadataExecutor;
    protected final ServiceDbFacade serviceDbFacade;

    protected QueryRequest replaceDatabaseInSql(QueryRequest request) {
        String sql = request.getSql().replaceAll("(?i) database", " schema");
        request.setSql(sql);
        return request;
    }

    protected String getSchemaName(QueryRequest request, String sqlNodeName) {
        int indexComma = sqlNodeName.indexOf(".");
        return indexComma == -1 ? request.getDatamartMnemonic() : sqlNodeName.substring(0, indexComma);
    }

    protected String getTableName(String sqlNodeName) {
        int indexComma = sqlNodeName.indexOf(".");
        return sqlNodeName.substring(indexComma + 1);
    }

    protected String getTableNameWithSchema(String schema, String tableName) {
        return schema + "." + tableName;
    }
}
