package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import ru.ibs.dtm.query.execution.core.utils.MetaDataQueryPreparer;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.QueryCostRequest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class TargetDatabaseDefinitionServiceImpl implements TargetDatabaseDefinitionService {

    private final DataSourcePluginService pluginService;

    @Override
    public void getTargetSource(QuerySourceRequest request, Handler<AsyncResult<QuerySourceRequest>> handler) {
        if (request.getSourceType() != null) {
            handler.handle(Future.succeededFuture(request));
        } else {
            getTargetSourceWithoutHint(request, handler);
        }
    }

    private void getTargetSourceWithoutHint(QuerySourceRequest request, Handler<AsyncResult<QuerySourceRequest>> handler) {
        if (CollectionUtils.isEmpty(MetaDataQueryPreparer.findInformationSchemaViews(request.getQueryRequest().getSql()))) {
            getTargetSourceFromCost(request, ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(
                            new QuerySourceRequest(
                                    request.getQueryRequest().copy(),
                                    request.getLogicalSchema(),
                                    ar.result())));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } else {
            handler.handle(Future.succeededFuture(
                    new QuerySourceRequest(
                            request.getQueryRequest().copy(),
                            SourceType.INFORMATION_SCHEMA)));
        }
    }

    private void getTargetSourceFromCost(QuerySourceRequest request, Handler<AsyncResult<SourceType>> handler) {
        List<Future> sourceTypeCost = new ArrayList<>();
        pluginService.getSourceTypes().forEach(sourceType -> {
            sourceTypeCost.add(Future.future(p -> {
                        val costRequest = new QueryCostRequest(request.getQueryRequest(), request.getLogicalSchema());
                        val costRequestContext = new QueryCostRequestContext(costRequest);
                        pluginService.calcQueryCost(sourceType, costRequestContext, costHandler -> {
                            if (costHandler.succeeded()) {
                                p.complete(Pair.of(sourceType, costHandler.result()));
                            } else {
                                p.fail(costHandler.cause());
                            }
                        });
                    })
            );
        });
        CompositeFuture.join(sourceTypeCost).onComplete(
                ar -> {
                    if (ar.succeeded()) {
                        SourceType sourceType = ar.result().list().stream()
                                .map(res -> (Pair<SourceType, Integer>) res)
                                .min(Comparator.comparingInt(Pair::getValue))
                                .map(Pair::getKey)
                                .orElse(null);
                        handler.handle(Future.succeededFuture(sourceType));
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
