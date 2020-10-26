package io.arenadata.dtm.query.execution.core.service.eddl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

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
