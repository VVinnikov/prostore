package io.arenadata.dtm.query.execution.core.service.config.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.plugin.api.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.config.ConfigExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.config.ConfigService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("coreConfigServiceImpl")
public class ConfigServiceImpl implements ConfigService<QueryResult> {

    public static final String NOT_SUPPORTED_CONFIG_QUERY_TYPE = "Not supported config query type";
    private final Map<SqlConfigType, ConfigExecutor> executorMap;

    @Autowired
    public ConfigServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public void execute(ConfigRequestContext context,
                        AsyncHandler<QueryResult> handler) {
        try {
            SqlConfigCall configCall = context.getSqlConfigCall();
            ConfigExecutor executor = executorMap.get(configCall.getSqlConfigType());
            if (executor != null) {
                executor.execute(context)
                    .onComplete(handler);
            } else {
                handler.handleError(NOT_SUPPORTED_CONFIG_QUERY_TYPE);
            }
        } catch (Exception e) {
            handler.handleError(NOT_SUPPORTED_CONFIG_QUERY_TYPE, e);
        }
    }

    @Override
    public void addExecutor(ConfigExecutor executor) {
        executorMap.put(executor.getConfigType(), executor);
    }

}
