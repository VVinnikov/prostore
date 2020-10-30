package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.SystemMetadata;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlUseSchema;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.utils.ParseQueryUtils;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class UseSchemaDmlExecutor implements DmlExecutor<QueryResult> {

    public static final String SCHEMA_COLUMN_NAME = "schema";
    private final DatamartDao datamartDao;
    private final ParseQueryUtils parseQueryUtils;

    @Autowired
    public UseSchemaDmlExecutor(ServiceDbFacade serviceDbFacade, ParseQueryUtils parseQueryUtils) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.parseQueryUtils = parseQueryUtils;
    }

    @Override
    public void execute(DmlRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        String datamart = parseQueryUtils.getDatamartName(((SqlUseSchema) context.getQuery()).getOperandList());
        datamartDao.existsDatamart(datamart)
                .onSuccess(isExists -> {
                    if (isExists) {
                        handler.handle(Future.succeededFuture(createQueryResult(context, datamart)));
                    } else {
                        handler.handle(Future.failedFuture(String.format("Datamart [%s] doesn't exist", datamart)));
                    }
                })
                .onFailure(error -> handler.handle(Future.failedFuture(error)));
    }

    @NotNull
    private QueryResult createQueryResult(DmlRequestContext context, String datamart) {
        final QueryResult result = new QueryResult();
        result.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        result.setMetadata(Collections.singletonList(new ColumnMetadata(SCHEMA_COLUMN_NAME, SystemMetadata.SCHEMA,
                ColumnType.VARCHAR)));
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(SCHEMA_COLUMN_NAME, datamart);
        result.setResult(Collections.singletonList(rowMap));
        return result;
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.OTHER;
    }
}
