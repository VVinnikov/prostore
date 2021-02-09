package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;

public interface RollbackService<T> extends DatamartExecutionService<RollbackRequest, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
