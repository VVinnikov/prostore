package ru.ibs.dtm.query.execution.core.service.delta;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaQuery;

/**
 * Экстрактор параметров delta запросов
 */
public interface DeltaQueryParamExtractor {

    /**
     * <p>Извелечь параметры</p>
     *
     * @param request            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void extract(QueryRequest request, Handler<AsyncResult<DeltaQuery>> asyncResultHandler);
}
