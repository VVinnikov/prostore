package ru.ibs.dtm.query.calcite.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import ru.ibs.dtm.common.dto.QueryParserRequest;
import ru.ibs.dtm.common.dto.QueryParserResponse;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;
import ru.ibs.dtm.query.calcite.core.service.QueryParserService;

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
    public void parse(QueryParserRequest request, Handler<AsyncResult<QueryParserResponse>> asyncResultHandler) {
        vertx.executeBlocking(it -> {
            try {
                val context = contextProvider.context(request.getSchema());
                try {
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
                    log.error("Request parsing error", e);
                    it.fail(e);
                }
            } catch (Exception e) {
                log.error("Request parsing error", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                asyncResultHandler.handle(Future.succeededFuture((QueryParserResponse) ar.result()));
            } else {
                log.debug("Error while executing the parse method", ar.cause());
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
