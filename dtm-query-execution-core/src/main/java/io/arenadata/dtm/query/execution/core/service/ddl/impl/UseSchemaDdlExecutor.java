package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.SystemMetadata;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class UseSchemaDdlExecutor extends QueryResultDdlExecutor {

    public static final String SCHEMA_COLUMN_NAME = "schema";
    private final DatamartDao datamartDao;

    public UseSchemaDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
    }

    @Override
    public void execute(DdlRequestContext context, String datamart, Handler<AsyncResult<QueryResult>> handler) {
        datamartDao.existsDatamart(datamart)
                .onSuccess(isExists -> {
                    if (isExists) {
                        context.setDatamartName(datamart);
                        handler.handle(Future.succeededFuture(createQueryResult(context)));
                    } else {
                        handler.handle(Future.failedFuture(String.format("Datamart [%s] doesn't exist", datamart)));
                    }
                })
                .onFailure(error -> handler.handle(Future.failedFuture(error)));
    }

    @NotNull
    private QueryResult createQueryResult(DdlRequestContext context) {
        final QueryResult result = new QueryResult();
        result.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        result.setMetadata(Collections.singletonList(new ColumnMetadata(SCHEMA_COLUMN_NAME, SystemMetadata.SCHEMA, ColumnType.VARCHAR)));
        List<Map<String, Object>> rows = new ArrayList<>();
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(SCHEMA_COLUMN_NAME, context.getDatamartName());
        rows.add(rowMap);
        result.setResult(rows);
        return result;
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.OTHER_DDL;
    }
}
