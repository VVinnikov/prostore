package ru.ibs.dtm.query.calcite.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.dto.QueryParserRequest;
import ru.ibs.dtm.common.dto.QueryParserResponse;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.calcite.core.delta.dto.DeltaInformation;
import ru.ibs.dtm.query.calcite.core.delta.service.DeltaInformationExtractor;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.calcite.core.service.QueryParserService;

@Slf4j
public class CalciteDMLQueryParserServiceImpl implements QueryParserService {
    private final DefinitionService<SqlNode> definitionService;
    private final CalciteContextProvider contextProvider;
    private final DeltaService deltaService;
    private final Vertx vertx;

    public CalciteDMLQueryParserServiceImpl(DefinitionService<SqlNode> definitionService,
                                            CalciteContextProvider contextProvider,
                                            DeltaService deltaService,
                                            Vertx vertx) {
        this.definitionService = definitionService;
        this.contextProvider = contextProvider;
        this.deltaService = deltaService;
        this.vertx = vertx;
    }

    @Override
    public void parse(QueryParserRequest request, Handler<AsyncResult<QueryParserResponse>> asyncResultHandler) {
        vertx.executeBlocking(it -> {
            try {
                if (request == null || StringUtils.isEmpty(request.getQueryRequest().getSql())) {
                    log.error("Неопределен запрос {}", request);
                    asyncResultHandler.handle(Future.failedFuture(String.format("Неопределен запрос %s", request)));
                    return;
                }
                val sqlNode = definitionService.processingQuery(request.getQueryRequest().getSql());
                val informationResult = DeltaInformationExtractor.extract(sqlNode, request.getQueryRequest().getDatamartMnemonic());
                calculateDeltaValues(informationResult.getDeltaInformations(), ar -> {
                    if (ar.succeeded()) {
                        val context = contextProvider.context(request.getSchema());
                        try {
                            String sql = informationResult.getSqlWithoutSnapshots();
                            val parse = context.getPlanner().parse(sql);
                            val validatedQuery = context.getPlanner().validate(parse);
                            val relQuery = context.getPlanner().rel(validatedQuery);
                            val copyRequest = request.getQueryRequest().copy();
                            copyRequest.setSql(sql);
                            it.complete(new QueryParserResponse(
                                    copyRequest,
                                    request.getSchema(),
                                    relQuery,
                                    parse
                            ));
                        } catch (Exception e) {
                            log.error("Ошибка разбора запроса", e);
                            it.fail(e);
                        }
                    } else {
                        asyncResultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });

            } catch (Exception e) {
                log.error("Ошибка парсинга запроса", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                asyncResultHandler.handle(Future.succeededFuture((QueryParserResponse) ar.result()));
            } else {
                log.debug("Ошибка при исполнении метода parse", ar.cause());
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void calculateDeltaValues(List<DeltaInformation> deltas,
                                      Handler<AsyncResult<List<DeltaInformation>>> handler) {
        val requests = deltas.stream()
                .map(d -> new ActualDeltaRequest(d.getSchemaName(), d.getDeltaTimestamp()))
                .collect(Collectors.toList());

        deltaService.getDeltasOnDateTimes(requests, ar -> {
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()));
            }
            List<Long> deltaNums = ar.result();
            List<DeltaInformation> result = IntStream.range(0, requests.size())
                    .mapToObj(i -> deltas.get(i).withDeltaNum(deltaNums.get(i)))
                    .collect(Collectors.toList());
            handler.handle(Future.succeededFuture(result));
        });
    }
}
