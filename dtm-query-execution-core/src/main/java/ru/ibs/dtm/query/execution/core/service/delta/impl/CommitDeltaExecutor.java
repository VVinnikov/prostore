package ru.ibs.dtm.query.execution.core.service.delta.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.delta.HotDelta;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaExecutor;
import ru.ibs.dtm.query.execution.core.service.delta.StatusEventPublisher;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.CommitDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction.COMMIT_DELTA;

@Component
@Slf4j
public class CommitDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private final Vertx vertx;
    private DeltaServiceDao deltaServiceDao;
    private DeltaQueryResultFactory deltaQueryResultFactory;

    @Autowired
    public CommitDeltaExecutor(ServiceDbFacade serviceDbFacade,
                               DeltaQueryResultFactory deltaQueryResultFactory,
                               @Qualifier("coreVertx") Vertx vertx) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.vertx = vertx;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
    }

    @Override
    public void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val commitDelta = (CommitDeltaQuery) context.getDeltaQuery();
        deltaServiceDao.writeDeltaHotSuccess(datamart, commitDelta.getDeltaDateTime())
                .onSuccess(committedDeltaNum -> {
                    DeltaRecord deltaRecord = createDeltaRecord(context, commitDelta, committedDeltaNum);
                    publishStatus(StatusEventCode.DELTA_CLOSE, datamart, deltaRecord);
                    QueryResult res = deltaQueryResultFactory.create(context, deltaRecord);
                    handler.handle(Future.succeededFuture(res));
                })
                .onFailure(err -> handler.handle(Future.failedFuture(err)));
    }

    private DeltaRecord createDeltaRecord(DeltaRequestContext context, CommitDeltaQuery commitDelta, Long deltaNum){
        DeltaRecord deltaRecord = new DeltaRecord();
        deltaRecord.setSinId(deltaNum);
        deltaRecord.setStatusDate(commitDelta.getDeltaDateTime());
        deltaRecord.setSysDate(getSysDate(context));
        deltaRecord.setStatusDate(LocalDateTime.now());
        deltaRecord.setStatus(DeltaLoadStatus.SUCCESS);
        return deltaRecord;
    }

    private LocalDateTime getSysDate(DeltaRequestContext context) {
        var sysDate = LocalDateTime.now(ZoneOffset.UTC);
        if (context.getDeltaQuery() instanceof CommitDeltaQuery) {
            CommitDeltaQuery query = (CommitDeltaQuery) context.getDeltaQuery();
            if (query != null && query.getDeltaDateTime() != null) {
                sysDate = query.getDeltaDateTime();
            }
        }
        return sysDate;
    }

    @Override
    public DeltaAction getAction() {
        return COMMIT_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
