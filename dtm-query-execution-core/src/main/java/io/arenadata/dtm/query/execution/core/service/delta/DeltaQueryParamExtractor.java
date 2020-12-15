package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;

/**
 * Экстрактор параметров delta запросов
 */
public interface DeltaQueryParamExtractor {

    /**
     * <p>Извелечь параметры</p>
     *  @param request            запрос
     * @param handler хэндлер асинхронной обработки результата
     */
    void extract(QueryRequest request, AsyncHandler<DeltaQuery> handler);
}
