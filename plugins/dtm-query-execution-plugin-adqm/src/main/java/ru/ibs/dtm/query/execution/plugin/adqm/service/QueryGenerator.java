package ru.ibs.dtm.query.execution.plugin.adqm.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.rel.RelRoot;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContext;

/**
 * Преобразователи DML запроса
 */
public interface QueryGenerator {
    /**
     * Преобразовать запрос
     */
    void mutateQuery(RelRoot sqlNode, Long selectOn, CalciteContext calciteContext, Handler<AsyncResult<String>> handler);
}
