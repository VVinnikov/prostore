package io.arenadata.dtm.query.execution.core.service.eddl;

import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.arenadata.dtm.query.execution.plugin.api.eddl.EddlRequestContext;
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
