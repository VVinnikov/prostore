package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class GetDeltaByDateTimeExecutor extends GetDeltaOkExecutor implements DeltaExecutor {

    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public GetDeltaByDateTimeExecutor(ServiceDbFacade serviceDbFacade,
                                      @Qualifier("deltaOkQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory) {
        super(serviceDbFacade, deltaQueryResultFactory);
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public void execute(DeltaQuery deltaQuery, AsyncHandler<QueryResult> handler) {
        getDeltaOk(deltaQuery)
                .onComplete(handler);
    }

    private Future<QueryResult> getDeltaOk(DeltaQuery deltaQuery) {
        return deltaServiceDao.getDeltaByDateTime(deltaQuery.getDatamart(), deltaQuery.getDeltaDate())
                .map(deltaOk -> createResult(deltaOk, deltaQuery));
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.GET_DELTA_BY_DATETIME;
    }
}
