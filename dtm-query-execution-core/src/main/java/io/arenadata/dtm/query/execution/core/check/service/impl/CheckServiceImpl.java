package io.arenadata.dtm.query.execution.core.check.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.service.CheckExecutor;
import io.arenadata.dtm.query.execution.core.check.service.CheckService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;
import org.tarantool.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Service("coreCheckService")
public class CheckServiceImpl implements CheckService {
    private final Map<CheckType, CheckExecutor> executorMap;

    public CheckServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        if (StringUtils.isEmpty(datamart) && context.getCheckType() != CheckType.VERSIONS) {
            return Future.failedFuture(
                    new DtmException("Datamart must be specified for all tables and views"));
        } else {
            return executorMap.get(context.getCheckType())
                    .execute(context);
        }
    }

    @Override
    public void addExecutor(CheckExecutor executor) {
        executorMap.put(executor.getType(), executor);
    }

}
