package io.arenadata.dtm.query.execution.core.service.eddl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.vertx.core.Future;

/**
 * EDDL query param extractor
 */
public interface EddlQueryParamExtractor {

    /**
     * <p>exctract params</p>
     *
     * @param request            request
     * @param asyncResultHandler async result handler
     * @return future object
     */
    Future<EddlQuery> extract(QueryRequest request);

}
