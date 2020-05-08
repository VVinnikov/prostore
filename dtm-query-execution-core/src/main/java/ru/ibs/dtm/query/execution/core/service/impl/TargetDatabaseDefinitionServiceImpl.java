package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import ru.ibs.dtm.query.execution.core.utils.HintExtractor;
import ru.ibs.dtm.query.execution.core.utils.MetaDataQueryPreparer;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class TargetDatabaseDefinitionServiceImpl implements TargetDatabaseDefinitionService {

  private final DataSourcePluginService pluginService;
  private final HintExtractor hintExtractor;

  @Autowired

  public TargetDatabaseDefinitionServiceImpl(DataSourcePluginService pluginService,
                                             HintExtractor hintExtractor) {
    this.pluginService = pluginService;
    this.hintExtractor = hintExtractor;
  }

  @Override
  public void getTargetSource(QueryRequest request, Handler<AsyncResult<QuerySourceRequest>> handler) {
    hintExtractor.extractHint(request, ar -> {
      if (ar.succeeded()) {
        QuerySourceRequest querySourceRequest = ar.result();
        if (querySourceRequest.getSourceType() != null) {
          handler.handle(Future.succeededFuture(querySourceRequest));
        } else {
          getTargetSourceWithoutHint(request, handler);
        }
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void getTargetSourceWithoutHint(QueryRequest request, Handler<AsyncResult<QuerySourceRequest>> handler) {
    if (CollectionUtils.isEmpty(MetaDataQueryPreparer.findInformationSchemaViews(request.getSql()))) {
      getTargetSourceFromCost(request, ar -> {
        if (ar.succeeded()) {
          handler.handle(Future.succeededFuture(
            new QuerySourceRequest(
              request.copy(),
              ar.result())));
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
    } else {
      handler.handle(Future.succeededFuture(
        new QuerySourceRequest(
          request.copy(),
          SourceType.INFORMATION_SCHEMA)));
    }
  }

  private void getTargetSourceFromCost(QueryRequest request, Handler<AsyncResult<SourceType>> handler) {
    List<Future> sourceTypeCost = new ArrayList<>();
    pluginService.getSourceTypes().forEach(sourceType -> {
      sourceTypeCost.add(Future.future(p ->
        pluginService.calcQueryCost(sourceType, new CalcQueryCostRequest(request), ar -> {
          if (ar.succeeded()) {
            p.complete(Pair.of(sourceType, ar.result()));
          } else {
            p.fail(ar.cause());
          }
        }))
      );
    });
    CompositeFuture.all(sourceTypeCost).onComplete(
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
