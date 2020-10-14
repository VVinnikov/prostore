package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.*;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import ru.ibs.dtm.query.execution.core.utils.MetaDataQueryPreparer;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.QueryCostRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class TargetDatabaseDefinitionServiceImpl implements TargetDatabaseDefinitionService {

    private final LogicalSchemaProvider logicalSchemaProvider;
    private final DataSourcePluginService pluginService;

    @Override
    public void getTargetSource(QuerySourceRequest request, Handler<AsyncResult<QuerySourceRequest>> handler) {
        if (request.getQueryRequest().getSourceType() != null) {
            getLogicalSchema(request)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        request.setLogicalSchema(ar.result());
                        handler.handle(Future.succeededFuture(request));
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                })
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } else {
            getTargetSourceWithoutHint(request, handler);
        }
    }

    private void getTargetSourceWithoutHint(QuerySourceRequest request, Handler<AsyncResult<QuerySourceRequest>> handler) {
        if (CollectionUtils.isEmpty(MetaDataQueryPreparer.findInformationSchemaViews(request.getQueryRequest().getSql()))) {
            getLogicalSchema(request)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        request.setLogicalSchema(ar.result());
                        getTargetSourceFromCost(request, tr -> {
                            if (tr.succeeded()) {
                                val sourceType = tr.result();
                                val queryRequestWithSourceType = request.getQueryRequest().copy();
                                queryRequestWithSourceType.setSourceType(sourceType);
                                handler.handle(Future.succeededFuture(
                                    new QuerySourceRequest(
                                        queryRequestWithSourceType,
                                        request.getLogicalSchema(),
                                        Collections.emptyList(),
                                                    sourceType)));
                                } else {
                                    handler.handle(Future.failedFuture(tr.cause()));
                                }
                            });
                        } else {
                            handler.handle(Future.failedFuture(ar.cause()));
                        }
                    })
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } else {
            val queryRequestWithSourceType = request.getQueryRequest().copy();
            queryRequestWithSourceType.setSourceType(SourceType.INFORMATION_SCHEMA);
            val result = new QuerySourceRequest(
                queryRequestWithSourceType,
                SourceType.INFORMATION_SCHEMA);
            handler.handle(Future.succeededFuture(result));
        }
    }

    private Future<List<Datamart>> getLogicalSchema(QuerySourceRequest request) {
        return Future.future((Promise<List<Datamart>> promise) -> logicalSchemaProvider.getSchema(request.getQueryRequest(), promise));
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
