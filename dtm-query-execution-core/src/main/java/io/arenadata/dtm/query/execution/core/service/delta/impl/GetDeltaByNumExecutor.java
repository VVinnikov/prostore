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

import java.util.Optional;

@Component
public class GetDeltaByNumExecutor extends GetDeltaOkExecutor implements DeltaExecutor {

    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public GetDeltaByNumExecutor(ServiceDbFacade serviceDbFacade,
                                 @Qualifier("deltaOkQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory) {
        super(serviceDbFacade, deltaQueryResultFactory);
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public void execute(DeltaQuery deltaQuery, Handler<AsyncResult<QueryResult>> handler) {
        getDeltaOk(deltaQuery)
                .onComplete(handler);
    }

    private Future<QueryResult> getDeltaOk(DeltaQuery deltaQuery) {
        return deltaServiceDao.getDeltaByNum(deltaQuery.getDatamart(), deltaQuery.getDeltaNum())
                .map(deltaOk -> createResult(deltaOk, deltaQuery));
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.GET_DELTA_BY_NUM;
    }
}
