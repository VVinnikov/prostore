package io.arenadata.dtm.query.execution.plugin.api.service.config;

import io.arenadata.dtm.query.execution.plugin.api.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import io.vertx.core.AsyncResult;


public interface ConfigService<T> extends DatamartExecutionService<ConfigRequestContext, AsyncResult<T>> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CONFIG;
    }

    void addExecutor(ConfigExecutor executor);
}
