package ru.ibs.dtm.query.execution.core.service.delta.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaExecutor;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.BeginDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction;

import java.time.LocalDateTime;
import java.util.UUID;

import static ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction.BEGIN_DELTA;

@Component
@Slf4j
public class BeginDeltaExecutor implements DeltaExecutor {

    private ServiceDao serviceDao;
    private DeltaQueryResultFactory deltaQueryResultFactory;

    @Autowired
    public BeginDeltaExecutor(ServiceDao serviceDao, DeltaQueryResultFactory deltaQueryResultFactory) {
        this.serviceDao = serviceDao;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
    }

    @Override
    public void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        serviceDao.getDeltaHotByDatamart(datamartMnemonic, deltaHandler -> {
                    if (deltaHandler.succeeded()) {
                        DeltaRecord deltaRecord = deltaHandler.result();
                        Long deltaHot = initAndCheckDeltaHot(context, handler, deltaRecord);
                        DeltaRecord newDelta = createNextDeltaRecord(deltaHot, datamartMnemonic);
                        serviceDao.insertDelta(newDelta, insDeltaHandler -> {
                            if (insDeltaHandler.succeeded()) {
                                QueryResult res = deltaQueryResultFactory.create(context, newDelta);
                                handler.handle(Future.succeededFuture(res));
                            } else {
                                handler.handle(Future.failedFuture(insDeltaHandler.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(deltaHandler.cause()));
                    }
                }
        );
    }

    private Long getDeltaHot(DeltaRecord deltaRecord) {
        switch (deltaRecord.getStatus()) {
            case SUCCESS:
                return deltaRecord.getSinId() + 1;
            case ERROR:
                return deltaRecord.getSinId();
            case IN_PROCESS:
            default:
                return null;
        }
    }

    @Nullable
    private Long initAndCheckDeltaHot(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler, DeltaRecord deltaRecord) {
        Long deltaHot = 0L;
        if (deltaRecord != null) {
            deltaHot = getDeltaHot(deltaRecord);
            if (deltaHot == null) {
                handler.handle(Future.failedFuture(new RuntimeException("Дельта находится в процессе загрузки!")));
            } else if (((BeginDeltaQuery) context.getDeltaQuery()).getDeltaNum() != null
                    && !((BeginDeltaQuery) context.getDeltaQuery()).getDeltaNum().equals(deltaHot)) {
                handler.handle(Future.failedFuture(new RuntimeException("Номера заданной дельты и актуальной не совпадают!")));
            }
        }
        return deltaHot;
    }

    @NotNull
    private DeltaRecord createNextDeltaRecord(Long deltaHot, String datamartMnemonic) {
        DeltaRecord newDelta = new DeltaRecord();
        newDelta.setSinId(deltaHot);
        newDelta.setDatamartMnemonic(datamartMnemonic);
        newDelta.setLoadProcId(UUID.randomUUID().toString());
        newDelta.setStatusDate(LocalDateTime.now());
        newDelta.setStatus(DeltaLoadStatus.IN_PROCESS);
        return newDelta;
    }

    @Override
    public DeltaAction getAction() {
        return BEGIN_DELTA;
    }
}
