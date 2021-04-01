package io.arenadata.dtm.query.execution.core.eddl.service;

import io.arenadata.dtm.query.execution.core.eddl.dto.EddlQuery;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlRequestContext;
import io.vertx.core.Future;

/**
 * EDDL query param extractor
 */
public interface EddlQueryParamExtractor {

    /**
     * @param context eddl request context
     * @return
     */
    Future<EddlQuery> extract(EddlRequestContext context);

}
