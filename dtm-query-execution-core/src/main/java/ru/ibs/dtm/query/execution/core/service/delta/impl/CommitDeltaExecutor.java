package ru.ibs.dtm.query.execution.core.service.delta.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaExecutor;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.CommitDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction;

import java.time.LocalDateTime;

import static ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction.COMMIT_DELTA;

@Component
@Slf4j
public class CommitDeltaExecutor implements DeltaExecutor {

    private ServiceDao serviceDao;
    private DeltaQueryResultFactory deltaQueryResultFactory;

    @Autowired
    public CommitDeltaExecutor(ServiceDao serviceDao, DeltaQueryResultFactory deltaQueryResultFactory) {
        this.serviceDao = serviceDao;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
    }

    @Override
    public void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        serviceDao.getDeltaHotByDatamart(datamartMnemonic, deltaHandler -> {
                    if (deltaHandler.succeeded()) {
                        DeltaRecord deltaHotRecord = deltaHandler.result();
                        log.debug("Найдена последняя дельта: {} для витрины: {}", deltaHotRecord,  datamartMnemonic);
                        if (!deltaHotRecord.getStatus().equals(DeltaLoadStatus.IN_PROCESS)) {
                            handler.handle(Future.failedFuture(new RuntimeException("По заданной дельте еще не завершена загрузка данных!")));
                        }
                        serviceDao.getDeltaActualBySinIdAndDatamart(datamartMnemonic, deltaHotRecord.getSinId() - 1, actualDeltaHandler -> {
                            if (actualDeltaHandler.succeeded()) {
                                DeltaRecord deltaActualRecord = deltaHandler.result();
                                log.debug("Найдена актуальная дельта: {} для витрины: {}", deltaActualRecord,  datamartMnemonic);
                                if (((CommitDeltaQuery) context.getDeltaQuery()).getDeltaDateTime() != null
                                        && (deltaActualRecord.getSysDate().isAfter(((CommitDeltaQuery) context.getDeltaQuery()).getDeltaDateTime())
                                        || deltaActualRecord.getSysDate().equals(((CommitDeltaQuery) context.getDeltaQuery()).getDeltaDateTime()))) {
                                    handler.handle(Future.failedFuture(new RuntimeException("Заданное время меньше или равно времени актуальной дельты!")));
                                }
                                deltaHotRecord.setStatusDate(LocalDateTime.now());
                                deltaHotRecord.setSysDate(LocalDateTime.now());
                                deltaHotRecord.setStatus(DeltaLoadStatus.SUCCESS);
                                serviceDao.updateDelta(deltaHotRecord, updDeltaHandler -> {
                                    if (updDeltaHandler.succeeded()) {
                                        log.debug("Обновлена дельта: {} для витрины: {}", deltaHotRecord,  datamartMnemonic);
                                        QueryResult res = deltaQueryResultFactory.create(context, deltaHotRecord);
                                        handler.handle(Future.succeededFuture(res));
                                    } else {
                                        handler.handle(Future.failedFuture(updDeltaHandler.cause()));
                                    }
                                });
                            } else {
                                handler.handle(Future.failedFuture(actualDeltaHandler.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(deltaHandler.cause()));
                    }
                }
        );
    }

    @Override
    public DeltaAction getAction() {
        return COMMIT_DELTA;
    }
}
