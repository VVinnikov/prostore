package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;

public interface LlrService<T> extends DatamartExecutionService<LlrRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.LLR;
    }

}
