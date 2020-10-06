package ru.ibs.dtm.query.execution.core.service.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;

/**
 * EDDL query param extractor
 */
public interface EddlQueryParamExtractor {

    /**
     * <p>exctract params</p>
     *
     * @param request            request
     * @param asyncResultHandler async result handler
     */
    void extract(QueryRequest request, Handler<AsyncResult<EddlQuery>> asyncResultHandler);

}
