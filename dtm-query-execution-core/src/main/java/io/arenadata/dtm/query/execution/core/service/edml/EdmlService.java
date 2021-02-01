package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.service.DatamartExecutionService;

public interface EdmlService<T> extends DatamartExecutionService<EdmlRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.EDML;
    }

}
