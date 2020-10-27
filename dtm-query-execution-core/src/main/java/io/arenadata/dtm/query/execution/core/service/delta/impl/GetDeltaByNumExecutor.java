package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
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
public class GetDeltaByNumExecutor implements DeltaExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;

    @Autowired
    public GetDeltaByNumExecutor(ServiceDbFacade serviceDbFacade,
                                 @Qualifier("deltaOkQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.deltaQueryResultFactory = deltaQueryResultFactory;
    }

    @Override
    public void execute(DeltaQuery deltaQuery, Handler<AsyncResult<QueryResult>> handler) {
        getDeltaOk(deltaQuery)
                .onComplete(handler);
    }

    private Future<QueryResult> getDeltaOk(DeltaQuery deltaQuery) {
        return Future.future(promise ->
                deltaServiceDao.getDeltaByNum(deltaQuery.getDatamart(), deltaQuery.getDeltaNum())
                        .onSuccess(deltaOk -> {
                            QueryResult res = deltaQueryResultFactory.create(createDeltaRecord(deltaOk,
                                    deltaQuery.getDatamart()));
                            res.setRequestId(deltaQuery.getRequestId());
                            promise.complete(res);
                        })
                        .onFailure(promise::fail));
    }

    private DeltaRecord createDeltaRecord(OkDelta deltaOk, String datamart) {
        return deltaOk == null ? null : DeltaRecord.builder()
                .datamart(datamart)
                .deltaNum(deltaOk.getDeltaNum())
                .cnFrom(deltaOk.getCnFrom())
                .cnTo(deltaOk.getCnTo())
                .deltaDate(deltaOk.getDeltaDate())
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.GET_DELTA_BY_NUM;
    }
}
