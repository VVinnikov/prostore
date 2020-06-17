package ru.ibs.dtm.query.execution.core.service.delta;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction;

/**
 * Исполнитель delta запросов
 */
public interface DeltaExecutor {

    /**
     * <p>Выполнить delta запрос</p>
     *
     * @param context              контекст запрсоа
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
