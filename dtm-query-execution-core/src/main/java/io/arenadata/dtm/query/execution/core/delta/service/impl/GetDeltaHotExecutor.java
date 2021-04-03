package io.arenadata.dtm.query.execution.core.delta.service.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.service.DeltaExecutor;
import io.vertx.core.Future;
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
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return getDeltaHot(deltaQuery);
    }

    private Future<QueryResult> getDeltaHot(DeltaQuery deltaQuery) {
        return deltaServiceDao.getDeltaHot(deltaQuery.getDatamart())
                .map(deltaHot -> createResult(deltaHot, deltaQuery));
    }

    private QueryResult createResult(HotDelta delta, DeltaQuery deltaQuery) {
        if (delta != null) {
            QueryResult queryResult = deltaQueryResultFactory.create(createDeltaRecord(delta,
                    deltaQuery.getDatamart()));
            queryResult.setRequestId(deltaQuery.getRequest().getRequestId());
            return queryResult;
        } else {
            QueryResult queryResult = deltaQueryResultFactory.createEmpty();
            queryResult.setRequestId(deltaQuery.getRequest().getRequestId());
            return queryResult;
        }
    }

    private DeltaRecord createDeltaRecord(HotDelta delta, String datamart) {
        return DeltaRecord.builder()
                .datamart(datamart)
                .deltaNum(delta.getDeltaNum())
                .cnFrom(delta.getCnFrom())
                .cnTo(delta.getCnTo())
                .cnMax(delta.getCnMax())
                .rollingBack(delta.isRollingBack())
                .writeOperationsFinished(delta.getWriteOperationsFinished())
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.GET_DELTA_HOT;
    }
}
