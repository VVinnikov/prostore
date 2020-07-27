package ru.ibs.dtm.query.calcite.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
        //FIXME do initialize ActualRequests which must contains attrs: isLatestUncommited, deltaNum, deltaDateTime,
        //deltaStartedIn, deltaFinishedIn

        val requests = deltas.stream()
                .map(d -> new ActualDeltaRequest(d.getSchemaName(), d.getDeltaTimestamp(), d.isLatestUncommitedDelta()))
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
