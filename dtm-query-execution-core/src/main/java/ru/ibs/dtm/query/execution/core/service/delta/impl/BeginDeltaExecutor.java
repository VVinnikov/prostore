package ru.ibs.dtm.query.execution.core.service.delta.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaExecutor;
import ru.ibs.dtm.query.execution.core.service.delta.StatusEventPublisher;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.BeginDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction;

import java.time.LocalDateTime;
import java.util.UUID;

import static ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction.BEGIN_DELTA;

@Component
@Slf4j
public class BeginDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private DeltaServiceDao deltaServiceDao;
    private DeltaQueryResultFactory deltaQueryResultFactory;
    private final Vertx vertx;

    @Autowired
    public BeginDeltaExecutor(ServiceDbFacade serviceDbFacade,
                              @Qualifier("beginDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                              @Qualifier("coreVertx") Vertx vertx) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.vertx = vertx;
    }

    @Override
    public void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val beginDelta = (BeginDeltaQuery) context.getDeltaQuery();
        deltaServiceDao.writeNewDeltaHot(datamart, beginDelta.getDeltaNum())
                .onSuccess(newDeltaHotNum -> {
                    try {
                        DeltaRecord newDelta = createNextDeltaRecord(newDeltaHotNum, datamart);
                        publishStatus(StatusEventCode.DELTA_OPEN, datamart, newDelta);
                        QueryResult res = deltaQueryResultFactory.create(context, newDelta);
                        handler.handle(Future.succeededFuture(res));
                    } catch (Exception e) {
                        handler.handle(Future.failedFuture(e));
                    }

                })
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
    }

    @NotNull
    private DeltaRecord createNextDeltaRecord(Long deltaHot, String datamartMnemonic) {
        return DeltaRecord.builder()
                .sinId(deltaHot)
                .datamartMnemonic(datamartMnemonic)
                .loadProcId(UUID.randomUUID().toString())
                .statusDate(LocalDateTime.now())
                .status(DeltaLoadStatus.IN_PROCESS)
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return BEGIN_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
