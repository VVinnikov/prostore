package io.arenadata.dtm.query.calcite.core.service.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;

@Slf4j
public abstract class CalciteDMLQueryParserService implements QueryParserService {
    private final CalciteContextProvider contextProvider;
    private final Vertx vertx;

    public CalciteDMLQueryParserService(CalciteContextProvider contextProvider,
                                        Vertx vertx) {
        this.contextProvider = contextProvider;
        this.vertx = vertx;
    }

    @Override
    public void parse(QueryParserRequest request, AsyncHandler<QueryParserResponse> handler) {
        vertx.executeBlocking(it -> {
            try {
                val context = contextProvider.context(extendSchemes(request.getSchema()));
                val sql = request.getQueryRequest().getSql();
                val parse = context.getPlanner().parse(sql);
                val validatedQuery = context.getPlanner().validate(parse);
                val relQuery = context.getPlanner().rel(validatedQuery);
                val copyRequest = request.getQueryRequest().copy();
                copyRequest.setSql(sql);
                it.complete(new QueryParserResponse(
                        context,
                        copyRequest,
                        request.getSchema(),
                        relQuery,
                        parse
                ));
            } catch (Exception e) {
                it.fail(new DtmException("Request parsing error", e));
            }
        }, ar -> {
            if (ar.succeeded()) {
                handler.handleSuccess((QueryParserResponse) ar.result());
            } else {
                handler.handleError(ar.cause());
            }
        });
    }

    protected List<Datamart> extendSchemes(List<Datamart> datamarts) {
        return datamarts;
    }
}
