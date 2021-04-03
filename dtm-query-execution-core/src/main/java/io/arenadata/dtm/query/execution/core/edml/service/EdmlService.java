package io.arenadata.dtm.query.execution.core.edml.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;

public interface EdmlService<T> extends DatamartExecutionService<EdmlRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.EDML;
    }

}
