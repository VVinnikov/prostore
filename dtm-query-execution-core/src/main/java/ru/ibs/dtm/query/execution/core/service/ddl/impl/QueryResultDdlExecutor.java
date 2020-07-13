package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import lombok.AllArgsConstructor;
import org.jetbrains.annotations.NotNull;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;

@AllArgsConstructor
public abstract class QueryResultDdlExecutor implements DdlExecutor<QueryResult> {
    protected final MetadataFactory<DdlRequestContext> metadataFactory;
    protected final MariaProperties mariaProperties;
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

    @NotNull
    protected String getSql(DdlRequestContext context, String sqlNodeName) {
        QueryRequest request = context.getRequest().getQueryRequest();
        String tableWithSchema = SqlPreparer.getTableWithSchema(mariaProperties.getOptions().getDatabase(), sqlNodeName);
        return SqlPreparer.removeDistributeBy(SqlPreparer.replaceQuote(SqlPreparer.replaceTableInSql(request.getSql(), tableWithSchema)));
    }
}
