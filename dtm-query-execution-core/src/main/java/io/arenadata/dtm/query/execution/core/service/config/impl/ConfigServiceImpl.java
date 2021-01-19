package io.arenadata.dtm.query.execution.core.service.config.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.query.execution.core.dto.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.config.ConfigExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.config.ConfigService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("coreConfigServiceImpl")
public class ConfigServiceImpl implements ConfigService<QueryResult> {

    private final Map<SqlConfigType, ConfigExecutor> executorMap;

    @Autowired
    public ConfigServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public Future<QueryResult> execute(ConfigRequestContext context) {
        return getExecutor(context)
                .compose(executor -> executor.execute(context));
    }

    private Future<ConfigExecutor> getExecutor(ConfigRequestContext context) {
        return Future.future(promise -> {
            SqlConfigCall configCall = context.getSqlNode();
            ConfigExecutor executor = executorMap.get(configCall.getSqlConfigType());
            if (executor != null) {
                promise.complete(executor);
            } else {
                promise.fail(new DtmException("Not supported config query type"));
            }
        });
    }

    @Override
    public void addExecutor(ConfigExecutor executor) {
        executorMap.put(executor.getConfigType(), executor);
    }

}
