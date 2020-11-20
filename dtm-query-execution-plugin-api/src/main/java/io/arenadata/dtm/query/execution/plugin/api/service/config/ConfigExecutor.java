package io.arenadata.dtm.query.execution.plugin.api.service.config;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.query.execution.plugin.api.config.ConfigRequestContext;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;

public interface ConfigExecutor {
    Future<QueryResult> execute(ConfigRequestContext context);

    SqlConfigType getConfigType();

    @Autowired
    default void register(ConfigService<?> service) {
        service.addExecutor(this);
    }
}
