package ru.ibs.dtm.query.calcite.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.calcite.core.util.DeltaInformationExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
public class DeltaQueryPreprocessorImpl implements DeltaQueryPreprocessor {
    private final DefinitionService<SqlNode> definitionService;
    private final DeltaService deltaService;

    public DeltaQueryPreprocessorImpl(DefinitionService<SqlNode> definitionService, DeltaService deltaService) {
        this.definitionService = definitionService;
        this.deltaService = deltaService;
    }

    @Override
    public Future<QueryRequest> process(QueryRequest request) {
        return Future.future(handler -> {
            try {
                if (request == null || StringUtils.isEmpty(request.getSql())) {
                    log.error("Неопределен запрос {}", request);
                    handler.fail(String.format("Неопределен запрос %s", request));
                } else {
                    val sqlNode = definitionService.processingQuery(request.getSql());
                    val deltaInfoRes = DeltaInformationExtractor.extract(sqlNode);
                    calculateDeltaValues(deltaInfoRes.getDeltaInformations(), ar -> {
                        if (ar.succeeded()) {
                            try {
                                QueryRequest copyRequest = request.copy();
                                copyRequest.setDeltaInformations(deltaInfoRes.getDeltaInformations());
                                copyRequest.setSql(deltaInfoRes.getSqlWithoutSnapshots());
                                copyRequest.setDeltaInformations(ar.result());
                                handler.complete(copyRequest);
                            } catch (Exception e) {
                                log.error("Ошибка разбора запроса", e);
                                handler.fail(e);
                            }
                        } else {
                            handler.fail(ar.cause());
                        }
                    });
                }
            } catch (Exception e) {
                log.error("Ошибка парсинга запроса", e);
                handler.fail(e);
            }
        });
    }

    private void calculateDeltaValues(List<DeltaInformation> deltas,
                                      Handler<AsyncResult<List<DeltaInformation>>> handler) {
        final Map<DeltaInformation, ActualDeltaRequest> deltaInfoMap = new HashMap<>();
        final Map<ActualDeltaRequest, Long> requestDeltaMap = new HashMap<>();
        initRequestDeltaMap(deltas, deltaInfoMap, requestDeltaMap);

        final List<ActualDeltaRequest> actualDeltaRequests = new ArrayList<>(requestDeltaMap.keySet());
        deltaService.getDeltasOnDateTimes(actualDeltaRequests, ar -> {
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()));
            }
            List<Long> deltaNums = ar.result();
            List<DeltaInformation> resultDeltas = new ArrayList<>();
            IntStream.range(0, actualDeltaRequests.size()).forEach(i ->
                    requestDeltaMap.put(actualDeltaRequests.get(i), deltaNums.get(i)));
            deltaInfoMap.forEach((k, v) -> {
                if (v != null) {
                    resultDeltas.add(k.withDeltaNum(requestDeltaMap.get(v)));
                } else {
                    resultDeltas.add(k);
                }
            });
            handler.handle(Future.succeededFuture(resultDeltas));
        });
    }

    private void initRequestDeltaMap(List<DeltaInformation> deltas, Map<DeltaInformation, ActualDeltaRequest> deltaInfoMap,
                                     Map<ActualDeltaRequest, Long> requestDeltaMap) {
        deltas.forEach(d -> {
            ActualDeltaRequest request = null;
            if (d.isLatestUncommitedDelta() || d.getDeltaTimestamp() != null) {
                request = new ActualDeltaRequest(d.getSchemaName(), d.getDeltaTimestamp(), d.isLatestUncommitedDelta());
                requestDeltaMap.put(request, null);
            }
            deltaInfoMap.put(d, request);
        });
    }
}
