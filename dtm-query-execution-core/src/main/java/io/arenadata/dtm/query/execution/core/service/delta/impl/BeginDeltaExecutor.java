package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.StatusEventPublisher;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.BEGIN_DELTA;

@Component
@Slf4j
public class BeginDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private static final String ERR_GETTING_QUERY_RESULT_MSG = "Error creating begin delta result";
    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final Vertx vertx;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public BeginDeltaExecutor(ServiceDbFacade serviceDbFacade,
                              @Qualifier("beginDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                              @Qualifier("coreVertx") Vertx vertx,
                              EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.vertx = vertx;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return Future.future(promise -> beginDelta(deltaQuery)
                .onSuccess(result -> {
                    evictQueryTemplateCacheService.evictByDatamartName(deltaQuery.getDatamart());
                    promise.complete(result);
                })
                .onFailure(promise::fail));
    }

    private Future<QueryResult> beginDelta(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            val beginDeltaQuery = (BeginDeltaQuery) deltaQuery;
            if (beginDeltaQuery.getDeltaNum() == null) {
                writeDeltaHot(beginDeltaQuery).onComplete(promise);
            } else {
                writeDeltaHotByNum(beginDeltaQuery).onComplete(promise);
            }
        });
    }

    private Future<QueryResult> writeDeltaHotByNum(BeginDeltaQuery beginDeltaQuery) {
        return Future.future(promise -> {
            deltaServiceDao.writeNewDeltaHot(beginDeltaQuery.getDatamart(), beginDeltaQuery.getDeltaNum())
                    .onSuccess(newDeltaHotNum -> {
                        try {
                            promise.complete(getDeltaQueryResult(newDeltaHotNum,
                                    beginDeltaQuery));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<QueryResult> writeDeltaHot(BeginDeltaQuery beginDeltaQuery) {
        return Future.future(promise -> {
            deltaServiceDao.writeNewDeltaHot(beginDeltaQuery.getDatamart())
                    .onSuccess(newDeltaHotNum -> {
                        try {
                            promise.complete(getDeltaQueryResult(newDeltaHotNum,
                                    beginDeltaQuery));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
    }

    private QueryResult getDeltaQueryResult(Long deltaHotNum, BeginDeltaQuery deltaQuery) {
        DeltaRecord deltaRecord = createDeltaRecord(deltaQuery.getDatamart(), deltaHotNum);
        publishStatus(StatusEventCode.DELTA_OPEN, deltaQuery.getDatamart(), deltaRecord);
        QueryResult res = deltaQueryResultFactory.create(deltaRecord);
        res.setRequestId(deltaQuery.getRequest().getRequestId());
        return res;
    }

    private DeltaRecord createDeltaRecord(String datamart, Long deltaNum) {
        return DeltaRecord.builder()
                .deltaNum(deltaNum)
                .datamart(datamart)
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
