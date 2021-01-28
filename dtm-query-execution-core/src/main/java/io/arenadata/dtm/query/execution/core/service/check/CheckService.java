package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.arenadata.dtm.query.execution.core.service.DatamartExecutionService;

public interface CheckService extends DatamartExecutionService<CheckContext, QueryResult> {

    String CHECK_RESULT_COLUMN_NAME = "check_result";

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CHECK;
    }

    void addExecutor(CheckExecutor executor);
}
