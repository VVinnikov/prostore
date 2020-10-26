package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

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
