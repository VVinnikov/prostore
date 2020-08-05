package ru.ibs.dtm.query.calcite.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.calcite.core.util.DeltaInformationExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
                    log.error("Unspecified request {}", request);
                    handler.fail(String.format("Undefined request%s", request));
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
                                log.error("Request parsing error", e);
                                handler.fail(e);
                            }
                        } else {
                            handler.fail(ar.cause());
                        }
                    });
                }
            } catch (Exception e) {
                log.error("Request parsing error", e);
                handler.fail(e);
            }
        });
    }

    private void calculateDeltaValues(List<DeltaInformation> deltas,
                                      Handler<AsyncResult<List<DeltaInformation>>> handler) {
        final Map<Integer, DeltaInformation> deltaResultMap = new TreeMap<>();
        final List<ActualDeltaRequest> actualDeltaRequests = createActualRequests(deltas, deltaResultMap);
        deltaService.getDeltasOnDateTimes(actualDeltaRequests, ar -> {
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()));
            }
            List<Long> deltaNums = ar.result();
            int deltaReqOrder = 0;
            for (Map.Entry<Integer, DeltaInformation> dMap : deltaResultMap.entrySet()) {
                if (dMap.getValue() == null) {
                    deltaResultMap.put(dMap.getKey(), deltas.get(dMap.getKey()).withDeltaNum(deltaNums.get(deltaReqOrder)));
                    deltaReqOrder++;
                }
            }
            handler.handle(Future.succeededFuture(new ArrayList<>(deltaResultMap.values())));
        });
    }

    private List<ActualDeltaRequest> createActualRequests(List<DeltaInformation> deltas,
                                                          Map<Integer, DeltaInformation> deltaResultMap) {
        final List<ActualDeltaRequest> actualDeltaRequests = new ArrayList<>();
        int order = 0;
        for (DeltaInformation d : deltas) {
            ActualDeltaRequest request = null;
            if (d.getType().equals(DeltaType.NUM) && d.getDeltaNum() == null) {
                request = new ActualDeltaRequest(d.getSchemaName(), d.getDeltaTimestamp(), d.isLatestUncommitedDelta());
                actualDeltaRequests.add(request);
            }
            deltaResultMap.put(order, request == null ? d : null);
            order++;
        }
        return actualDeltaRequests;
    }
}
