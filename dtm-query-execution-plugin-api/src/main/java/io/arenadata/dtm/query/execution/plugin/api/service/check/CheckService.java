package io.arenadata.dtm.query.execution.plugin.api.service.check;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;

public interface CheckService extends DatamartExecutionService<CheckContext, QueryResult> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CHECK;
    }

    void addExecutor(CheckExecutor executor);
}
