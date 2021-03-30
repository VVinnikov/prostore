package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.exception.DeltaRangeInvalidException;
import io.arenadata.dtm.common.service.DeltaService;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class DeltaServiceExternalImpl implements DeltaService {

    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public DeltaServiceExternalImpl(ServiceDbFacade serviceDbFacade) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public Future<Long> getCnToByDeltaDatetime(String datamart, LocalDateTime dateTime) {
        return Future.future(handler -> deltaServiceDao.getDeltaByDateTime(datamart, dateTime)
                .onSuccess(delta -> handler.handle(Future.succeededFuture(delta.getCnTo())))
                .onFailure(err -> handler.handle(Future.succeededFuture(-1L))));
    }

    @Override
    public Future<Long> getCnToByDeltaNum(String datamart, long num) {
        return Future.future(handler -> deltaServiceDao.getDeltaByNum(datamart, num)
                .onSuccess(delta -> handler.handle(Future.succeededFuture(delta.getCnTo())))
                .onFailure(err -> handler.handle(Future.succeededFuture(-1L))));
    }

    @Override
    public Future<Long> getCnToDeltaHot(String datamart) {
        return Future.future(handler -> deltaServiceDao.getDeltaHot(datamart)
                .onSuccess(deltaHot -> {
                    if (deltaHot != null && deltaHot.getCnTo() != null && deltaHot.getCnTo() >= 0) {
                        handler.handle(Future.succeededFuture(deltaHot.getCnTo()));
                    } else {
                        deltaServiceDao.getDeltaOk(datamart)
                                .onSuccess(okDelta -> {
                                    if (okDelta != null) {
                                        handler.handle(Future.succeededFuture(okDelta.getCnTo()));
                                    } else {
                                        handler.handle(Future.succeededFuture(-1L));
                                    }
                                })
                                .onFailure(handler::fail);
                    }
                })
                .onFailure(handler::fail));
    }

    @Override
    public Future<SelectOnInterval> getCnFromCnToByDeltaNums(String datamart, long deltaFrom, long deltaTo) {
        return Future.future(handler -> CompositeFuture.join(deltaServiceDao.getDeltaByNum(datamart, deltaFrom), deltaServiceDao.getDeltaByNum(datamart, deltaTo))
                .onSuccess(ar -> {
                    Long cnFrom = ((OkDelta) ar.resultAt(0)).getCnFrom();
                    Long cnTo = ((OkDelta) ar.resultAt(1)).getCnTo();
                    handler.handle(Future.succeededFuture(new SelectOnInterval(cnFrom, cnTo)));
                })
                .onFailure(err -> {
                    val ex = new DeltaRangeInvalidException(String.format("Invalid delta range [%d, %d]", deltaFrom, deltaTo), err);
                    handler.handle(Future.failedFuture(ex));
                }));
    }
}
