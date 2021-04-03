package io.arenadata.dtm.query.execution.core.config.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.config.dto.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;

public interface ConfigService<T> extends DatamartExecutionService<ConfigRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CONFIG;
    }

    void addExecutor(ConfigExecutor executor);
}
