package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.SystemMetadata;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.*;

@Component
@Slf4j
public class UseSchemaDdlExecutor extends QueryResultDdlExecutor {

    public static final String SCHEMA_COLUMN_NAME = "schema";

    public UseSchemaDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                MariaProperties mariaProperties,
                                ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(sqlNodeName, ar -> {
            if (ar.succeeded()) {
                context.setDatamartName(sqlNodeName);
                handler.handle(Future.succeededFuture(createQueryResult(context)));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
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
