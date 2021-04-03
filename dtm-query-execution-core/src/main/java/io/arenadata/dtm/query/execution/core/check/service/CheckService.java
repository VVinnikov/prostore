package io.arenadata.dtm.query.execution.core.check.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;

public interface CheckService extends DatamartExecutionService<CheckContext, QueryResult> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CHECK;
    }

    void addExecutor(CheckExecutor executor);
}
