package ru.ibs.dtm.query.execution.core.service.delta;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaExecutor;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaService;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaQuery;

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
        deltaQueryParamExtractor.extract(context.getRequest().getQueryRequest(), exParamHandler -> {
            if (exParamHandler.succeeded()) {
                DeltaQuery deltaQuery = exParamHandler.result();
                context.setDeltaQuery(deltaQuery);
                executors.get(deltaQuery.getDeltaAction())
                        .execute(context, deltaExecHandler -> {
                            if (deltaExecHandler.succeeded()) {
                                QueryResult queryDeltaResult = deltaExecHandler.result();
                                log.debug("Результат выполнения запроса: {}, queryResult : {}",
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
