package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

public interface EdmlService<T> extends DatamartExecutionService<EdmlRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.EDML;
    }

}
