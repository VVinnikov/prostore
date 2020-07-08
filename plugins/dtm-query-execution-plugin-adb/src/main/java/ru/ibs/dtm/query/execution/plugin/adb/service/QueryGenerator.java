package ru.ibs.dtm.query.execution.plugin.adb.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.List;
import org.apache.calcite.rel.RelRoot;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.delta.DeltaInformation;

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
