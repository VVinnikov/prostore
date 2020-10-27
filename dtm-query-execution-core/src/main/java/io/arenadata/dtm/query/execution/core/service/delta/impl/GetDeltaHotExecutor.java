package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class GetDeltaHotExecutor implements DeltaExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;

    @Autowired
    public GetDeltaHotExecutor(ServiceDbFacade serviceDbFacade,
                               @Qualifier("deltaHotQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.deltaQueryResultFactory = deltaQueryResultFactory;
    }

    @Override
    public void execute(DeltaQuery deltaQuery, Handler<AsyncResult<QueryResult>> handler) {
        getDeltaHot(deltaQuery)
                .onComplete(handler);
    }

    private Future<QueryResult> getDeltaHot(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            deltaServiceDao.getDeltaHot(deltaQuery.getDatamart())
                    .onSuccess(deltaHot -> {
                        QueryResult res = deltaQueryResultFactory.create(createDeltaRecord(deltaHot,
                                deltaQuery.getDatamart()));
                        res.setRequestId(deltaQuery.getRequestId());
                        promise.complete(res);
                    })
                    .onFailure(promise::fail);
        });
    }

    private DeltaRecord createDeltaRecord(HotDelta deltaHot, String datamart) {
        return deltaHot == null ? null : DeltaRecord.builder()
                .datamart(datamart)
                .deltaNum(deltaHot.getDeltaNum())
                .cnFrom(deltaHot.getCnFrom())
                .cnTo(deltaHot.getCnTo())
                .cnMax(deltaHot.getCnMax())
                .rollingBack(deltaHot.isRollingBack())
                .writeOperationsFinished(deltaHot.getWriteOperationsFinished())
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.GET_DELTA_HOT;
    }
}
