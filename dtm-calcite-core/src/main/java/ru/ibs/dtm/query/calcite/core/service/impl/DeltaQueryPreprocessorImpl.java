package ru.ibs.dtm.query.calcite.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.common.delta.SelectOnInterval;
import ru.ibs.dtm.common.exception.DeltaRangeInvalidException;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.calcite.core.util.DeltaInformationExtractor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DeltaQueryPreprocessorImpl implements DeltaQueryPreprocessor {
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
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

    private void calculateDeltaValues(List<DeltaInformation> deltas, Handler<AsyncResult<List<DeltaInformation>>> handler) {
        final List<DeltaInformation> deltaResult = new ArrayList<>();
        deltas.forEach(deltaInformation -> {
            if (deltaInformation.isLatestUncommitedDelta()) {
                deltaService.getCnToDeltaHot(deltaInformation.getSchemaName())
                        .onSuccess(deltaCnTo -> {
                            deltaInformation.setSelectOnNum(deltaCnTo);
                        });
            } else {
                if (deltaInformation.getType().equals(DeltaType.FINISHED_IN) || deltaInformation.getType().equals(DeltaType.STARTED_IN)) {
                    calculateSelectOnInterval(deltaInformation, ar -> {
                        if (ar.succeeded()) {
                            deltaInformation.setSelectOnInterval(ar.result());
                            deltaResult.add(deltaInformation);
                        } else {
                            handler.handle(Future.failedFuture(ar.cause()));
                        }
                    });
                } else {
                    calculateSelectOnNum(deltaInformation, ar -> {
                        deltaInformation.setSelectOnNum(ar.result());
                        deltaResult.add(deltaInformation);
                    });
                }
            }
        });
        handler.handle(Future.succeededFuture(deltaResult));
    }

    private void calculateSelectOnNum(DeltaInformation deltaInformation, Handler<AsyncResult<Long>> handler){
        switch (deltaInformation.getType()) {
            case NUM:
                deltaService.getCnToByDeltaNum(deltaInformation.getSchemaName(), deltaInformation.getDeltaNum())
                        .onComplete(res -> handler.handle(res));
                break;
            case DATETIME:
                deltaService.getCnToByDeltaDatetime(deltaInformation.getSchemaName(), LocalDateTime.parse(deltaInformation.getDeltaTimestamp().replace("\'", ""), formatter))
                        .onComplete(res -> handler.handle(res));
                break;
            default:
                break;
        }
    }

    private void calculateSelectOnInterval(DeltaInformation deltaInformation, Handler<AsyncResult<SelectOnInterval>> handler){
        Long deltaFrom = deltaInformation.getDeltaInterval().getDeltaFrom();
        Long deltaTo = deltaInformation.getDeltaInterval().getDeltaTo();
        deltaService.getCnFromCnToByDeltaNums(deltaInformation.getSchemaName(), deltaFrom, deltaTo)
                .onComplete(res -> handler.handle(res));
    }
}
