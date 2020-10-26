package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaAction;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Исполнитель delta запросов
 */
public interface DeltaExecutor {

    /**
     * <p>Выполнить delta запрос</p>
     *
     * @param context            контекст запрсоа
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * Получить тип delta запроса
     *
     * @return тип запроса
     */
    DeltaAction getAction();
}
