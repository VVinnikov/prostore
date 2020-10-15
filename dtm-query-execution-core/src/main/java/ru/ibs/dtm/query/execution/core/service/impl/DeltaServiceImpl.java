package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.delta.SelectOnInterval;
import ru.ibs.dtm.common.exception.DeltaRangeInvalidException;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.OkDelta;

import java.time.LocalDateTime;

@Service
public class DeltaServiceImpl implements DeltaService {

    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public DeltaServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public Future<Long> getCnToByDeltaDatetime(String datamart, LocalDateTime dateTime){
        return Future.future(handler -> deltaServiceDao.getDeltaByDateTime(datamart, dateTime)
                .onSuccess(delta -> handler.handle(Future.succeededFuture(delta.getCnTo())))
                .onFailure(err -> handler.handle(Future.succeededFuture(-1L))));
    }

    @Override
    public Future<Long> getCnToByDeltaNum(String datamart, long num){
        return Future.future(handler -> deltaServiceDao.getDeltaByNum(datamart, num)
                .onSuccess(delta -> handler.handle(Future.succeededFuture(delta.getCnTo())))
                .onFailure(err -> handler.handle(Future.succeededFuture(-1L))));
    }

    @Override
    public Future<Long> getCnToDeltaHot(String datamart) {
        return Future.future(handler -> deltaServiceDao.getDeltaHot(datamart)
                .onSuccess(deltaHot -> handler.handle(Future.succeededFuture(deltaHot.getCnTo())))
                .onFailure(err1 ->
                    deltaServiceDao.getDeltaOk(datamart)
                            .onSuccess(res -> handler.handle(Future.succeededFuture(res.getCnTo())))
                            .onFailure(err2 -> handler.handle(Future.succeededFuture(-1L)))
                ));
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
