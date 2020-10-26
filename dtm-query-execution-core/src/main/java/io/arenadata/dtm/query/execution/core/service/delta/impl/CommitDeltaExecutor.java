package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.delta.DeltaLoadStatus;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.StatusEventPublisher;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaAction;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaAction.COMMIT_DELTA;

@Component
@Slf4j
public class CommitDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final DeltaServiceDao deltaServiceDao;
    private final Vertx vertx;

    @Autowired
    public CommitDeltaExecutor(ServiceDbFacade serviceDbFacade,
                               @Qualifier("commitDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
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
                    try {
                    DeltaRecord deltaRecord = createDeltaRecord(context, committedDeltaNum);
                    publishStatus(StatusEventCode.DELTA_CLOSE, datamart, deltaRecord);
                    QueryResult res = deltaQueryResultFactory.create(context, deltaRecord);
                    handler.handle(Future.succeededFuture(res));
                    } catch (Exception e) {
                        handler.handle(Future.failedFuture(e));
                    }
                })
                .onFailure(err -> handler.handle(Future.failedFuture(err)));
    }

    private DeltaRecord createDeltaRecord(DeltaRequestContext context, Long deltaNum){
        return DeltaRecord.builder()
                .sinId(deltaNum)
                .statusDate(LocalDateTime.now())
                .sysDate(getSysDate(context))
                .status(DeltaLoadStatus.SUCCESS)
                .build();
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
