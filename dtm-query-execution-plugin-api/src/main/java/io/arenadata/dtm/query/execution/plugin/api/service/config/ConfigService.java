package io.arenadata.dtm.query.execution.plugin.api.service.config;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;

public interface ConfigService<T> extends DatamartExecutionService<ConfigRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CONFIG;
    }

    void addExecutor(ConfigExecutor executor);
}
