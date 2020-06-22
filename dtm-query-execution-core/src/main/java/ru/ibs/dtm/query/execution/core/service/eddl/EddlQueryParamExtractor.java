package ru.ibs.dtm.query.execution.core.service.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;

/**
 * Экстрактор параметров eddl запроса
 */
public interface EddlQueryParamExtractor {

    /**
     * <p>Извелечь параметры</p>
     *
     * @param request            запрос
     * @param asyncResultHandler хэндлер асинхронной обработки результата
     */
    void extract(QueryRequest request, Handler<AsyncResult<EddlQuery>> asyncResultHandler);

}
