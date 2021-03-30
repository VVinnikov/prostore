package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Future;

public interface LlrService<T> extends DatamartExecutionService<LlrRequest, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.LLR;
    }

    Future<Void> prepare(LlrRequest request);
}
