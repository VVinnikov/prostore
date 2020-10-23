package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.rel.RelRoot;

import java.util.List;

/**
 * Преобразователи DML запроса
 */
public interface QueryGenerator {
    /**
     * Преобразовать запрос
     */
    void mutateQuery(RelRoot sqlNode,
                     List<DeltaInformation> deltaInformations,
                     CalciteContext calciteContext,
                     Handler<AsyncResult<String>> handler);
}
