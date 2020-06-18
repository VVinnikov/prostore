package ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.cost.QueryCostAlgorithm;
import ru.ibs.dtm.query.execution.plugin.adb.service.AdbQueryCostService;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostAnalyzer;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("adbQueryCostService")
public class AdbQueryCostServiceImpl implements AdbQueryCostService<Integer> {
    private final Map<QueryCostAlgorithm, QueryCostAnalyzer<Integer, ?>> analyzerMap;

    public AdbQueryCostServiceImpl() {
        analyzerMap = new HashMap<>();
    }

    @Override
    public void calc(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
        val algorithm = context.getRequest().getAlgorithm();
        if (analyzerMap.containsKey(algorithm)) {
            analyzerMap.get(algorithm).analyze(context, handler);
        } else {
            String errMsg = "QueryCostAnalyzer not found by algorithm " + algorithm;
            log.error(errMsg);
            handler.handle(Future.failedFuture(errMsg));
        }
    }

    @Override
    public <S extends QueryCostService<Integer>> void addAnalyzer(QueryCostAnalyzer<Integer, S> analyzer) {
        analyzerMap.put(analyzer.getAlgorithm(), analyzer);
    }

    @Override
    public void execute(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
        handler.handle(Future.failedFuture(new RuntimeException("Unsupported operation")));
    }
}
