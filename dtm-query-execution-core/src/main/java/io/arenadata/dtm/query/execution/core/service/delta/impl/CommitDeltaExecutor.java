package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.StatusEventPublisher;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.COMMIT_DELTA;

@Component
@Slf4j
public class CommitDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private static final String ERR_GETTING_QUERY_RESULT_MSG = "Error creating commit delta result";
    private final Vertx vertx;
    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public CommitDeltaExecutor(ServiceDbFacade serviceDbFacade,
                               @Qualifier("commitDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                               @Qualifier("coreVertx") Vertx vertx,
                               EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.vertx = vertx;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return commitDelta(deltaQuery);
    }

    private Future<QueryResult> commitDelta(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            val commitDeltaQuery = (CommitDeltaQuery) deltaQuery;
            if (commitDeltaQuery.getDeltaDate() == null) {
                writeDeltaHot(commitDeltaQuery).onComplete(promise);
            } else {
                writeDeltaHotByDate(commitDeltaQuery).onComplete(promise);
            }
        });
    }

    private Future<QueryResult> writeDeltaHotByDate(CommitDeltaQuery commitDeltaQuery) {
        return Future.future(promise -> {
            try {
                evictQueryTemplateCacheService.evictByDatamartName(commitDeltaQuery.getDatamart());
            } catch (Exception e) {
                promise.fail(new DtmException("Evict cache error"));
            }
            deltaServiceDao.writeDeltaHotSuccess(commitDeltaQuery.getDatamart(), commitDeltaQuery.getDeltaDate())
                    .onSuccess(deltaDate -> {
                        try {
                            promise.complete(getQueryResult(commitDeltaQuery, deltaDate));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<QueryResult> writeDeltaHot(CommitDeltaQuery commitDeltaQuery) {
        return Future.future(promise -> {
            try {
                evictQueryTemplateCacheService.evictByDatamartName(commitDeltaQuery.getDatamart());
            } catch (Exception e) {
                promise.fail(new DtmException("Evict cache error"));
            }
            deltaServiceDao.writeDeltaHotSuccess(commitDeltaQuery.getDatamart())
                    .onSuccess(deltaDate -> {
                        try {
                            promise.complete(getQueryResult(commitDeltaQuery, deltaDate));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
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
