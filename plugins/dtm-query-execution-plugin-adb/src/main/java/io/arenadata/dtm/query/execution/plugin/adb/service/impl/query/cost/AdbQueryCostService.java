package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryCostService;
import io.vertx.core.Future;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service("adbQueryCostService")
public class AdbQueryCostService implements QueryCostService<Integer> {

    @Override
    public Future<Integer> calc(QueryCostRequest request) {
        return Future.succeededFuture(0);
    }

    @Override
    public Future<Integer> execute(QueryCostRequest request) {
        return Future.failedFuture(new DataSourceException("Unsupported operation"));
    }
}
