package io.arenadata.dtm.query.execution.core.service.config;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.dto.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.service.DatamartExecutionService;

public interface ConfigService<T> extends DatamartExecutionService<ConfigRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CONFIG;
    }

    void addExecutor(ConfigExecutor executor);
}
