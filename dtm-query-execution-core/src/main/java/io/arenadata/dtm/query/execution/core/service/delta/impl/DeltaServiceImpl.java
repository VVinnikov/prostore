package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaService;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("coreDeltaService")
@Slf4j
public class DeltaServiceImpl implements DeltaService<QueryResult> {

    private final Map<DeltaAction, DeltaExecutor> executors;
    private final DeltaQueryParamExtractor deltaQueryParamExtractor;

    @Autowired
    public DeltaServiceImpl(DeltaQueryParamExtractor deltaQueryParamExtractor, List<DeltaExecutor> deltaExecutorList) {
        this.deltaQueryParamExtractor = deltaQueryParamExtractor;
        this.executors = deltaExecutorList.stream()
                .collect(Collectors.toMap(DeltaExecutor::getAction, it -> it));
    }

    @Override
    public void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        if (StringUtils.isEmpty(context.getRequest().getQueryRequest().getDatamartMnemonic())) {
            String errMsg = "Datamart must be not empty!\n" +
                    "For setting datamart you can use the following command: \"USE datamartName\"";
            log.error(errMsg);
            handler.handle(Future.failedFuture(errMsg));
        } else {
            executeDeltaRequest(context, handler);
        }
    }

    private void executeDeltaRequest(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        deltaQueryParamExtractor.extract(context.getRequest().getQueryRequest(), exParamHandler -> {
            if (exParamHandler.succeeded()) {
                DeltaQuery deltaQuery = exParamHandler.result();
                context.setDeltaQuery(deltaQuery);
                executors.get(deltaQuery.getDeltaAction())
                        .execute(context, deltaExecHandler -> {
                            if (deltaExecHandler.succeeded()) {
                                QueryResult queryDeltaResult = deltaExecHandler.result();
                                log.debug("Query result: {}, queryResult : {}",
                                        context.getRequest().getQueryRequest(), queryDeltaResult);
                                handler.handle(Future.succeededFuture(queryDeltaResult));
                            } else {
                                log.error(deltaExecHandler.cause().getMessage());
                                handler.handle(Future.failedFuture(deltaExecHandler.cause()));
                            }
                        });
            } else {
                handler.handle(Future.failedFuture(exParamHandler.cause()));
            }
        });
    }
}
