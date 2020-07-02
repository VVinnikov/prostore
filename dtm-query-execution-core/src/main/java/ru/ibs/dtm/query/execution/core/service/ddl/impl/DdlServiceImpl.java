package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.service.impl.CalciteDefinitionService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("coreDdlService")
public class DdlServiceImpl implements DdlService<QueryResult> {

    private final CalciteDefinitionService calciteDefinitionService;
    private final Map<SqlKind, DdlExecutor<QueryResult>> executorMap;
    private final Vertx vertx;

    @Autowired
    public DdlServiceImpl(CalciteDefinitionService calciteDefinitionService,
                          @Qualifier("coreVertx") Vertx vertx
    ) {
        this.calciteDefinitionService = calciteDefinitionService;
        this.vertx = vertx;
        executorMap = new HashMap<>();
    }

    @Override
    public void execute(DdlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        vertx.executeBlocking(it -> {
            try {
                SqlNode node = calciteDefinitionService.processingQuery(context.getRequest().getQueryRequest().getSql());
                it.complete(node);
            } catch (Exception e) {
                log.error("Ошибка парсинга запроса", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                execute(context, asyncResultHandler, ar);
            } else {
                log.debug("Ошибка исполнения", ar.cause());
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void execute(DdlRequestContext context, Handler<AsyncResult<QueryResult>> handler, AsyncResult<Object> ar) {
        if (ar.result() instanceof SqlDdl) {
            SqlDdl sqlDdl = ((SqlDdl) ar.result());
            String sqlNodeName = sqlDdl.getOperandList().stream().filter(t -> t instanceof SqlIdentifier).findFirst().get().toString();
            if (executorMap.containsKey(sqlDdl.getKind())) {
                executorMap.get(sqlDdl.getKind()).execute(context, sqlNodeName, handler);
            } else {
                log.error("Не поддерживаемый тип DDL запроса");
                handler.handle(Future.failedFuture(String.format("Не поддерживаемый тип DDL запроса [%s]", context)));
            }
        } else {
            log.error("Не поддерживаемый тип запроса");
            handler.handle(Future.failedFuture(String.format("Не поддерживаемый тип запроса [%s]", context)));
        }
    }

    @Override
    public void addExecutor(DdlExecutor<QueryResult> executor) {
        executorMap.put(executor.getSqlKind(), executor);
    }
}
