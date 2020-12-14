package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.exception.DtmException;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.StatusEventPublisher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.COMMIT_DELTA;

@Component
@Slf4j
public class CommitDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private static final String ERR_GETTING_QUERY_RESULT_MSG = "Error creating commit delta result";
    private final Vertx vertx;
    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;

    @Autowired
    public CommitDeltaExecutor(ServiceDbFacade serviceDbFacade,
                               @Qualifier("commitDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                               @Qualifier("coreVertx") Vertx vertx) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.vertx = vertx;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
    }

    @Override
    public void execute(DeltaQuery deltaQuery, Handler<AsyncResult<QueryResult>> handler) {
        val commitDeltaQuery = (CommitDeltaQuery) deltaQuery;
        if (commitDeltaQuery.getDeltaDate() == null) {
            deltaServiceDao.writeDeltaHotSuccess(commitDeltaQuery.getDatamart())
                    .onSuccess(deltaDate -> {
                        try {
                            QueryResult res = getQueryResult(commitDeltaQuery, deltaDate);
                            handler.handle(Future.succeededFuture(res));
                        } catch (Exception e) {
                            handler.handle(Future.failedFuture(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e)));
                        }
                    })
                    .onFailure(err -> handler.handle(Future.failedFuture(err)));
        } else {
            deltaServiceDao.writeDeltaHotSuccess(commitDeltaQuery.getDatamart(), commitDeltaQuery.getDeltaDate())
                    .onSuccess(deltaDate -> {
                        try {
                            QueryResult res = getQueryResult(commitDeltaQuery, deltaDate);
                            handler.handle(Future.succeededFuture(res));
                        } catch (Exception e) {
                            handler.handle(Future.failedFuture(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e)));
                        }
                    })
                    .onFailure(err -> handler.handle(Future.failedFuture(err)));
        }
    }

    private QueryResult getQueryResult(CommitDeltaQuery commitDeltaQuery, LocalDateTime deltaDate) {
        DeltaRecord deltaRecord = createDeltaRecord(commitDeltaQuery.getDatamart(), deltaDate);
        publishStatus(StatusEventCode.DELTA_CLOSE, commitDeltaQuery.getDatamart(), deltaRecord);
        QueryResult res = deltaQueryResultFactory.create(deltaRecord);
        res.setRequestId(commitDeltaQuery.getRequest().getRequestId());
        return res;
    }

    private DeltaRecord createDeltaRecord(String datamart, LocalDateTime deltaDate) {
        return DeltaRecord.builder()
                .datamart(datamart)
                .deltaDate(deltaDate)
                .build();
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
