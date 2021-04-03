package io.arenadata.dtm.query.execution.core.delta.service.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.service.DeltaExecutor;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

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
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return getDeltaOk(deltaQuery);
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
