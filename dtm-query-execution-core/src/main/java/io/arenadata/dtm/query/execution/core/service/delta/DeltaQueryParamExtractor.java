package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.vertx.core.Future;

/**
 * Экстрактор параметров delta запросов
 */
public interface DeltaQueryParamExtractor {

    /**
     * <p>Извелечь параметры</p>
     *
     * @param request запрос
     * @return future object
     */
    Future<DeltaQuery> extract(QueryRequest request);
}
