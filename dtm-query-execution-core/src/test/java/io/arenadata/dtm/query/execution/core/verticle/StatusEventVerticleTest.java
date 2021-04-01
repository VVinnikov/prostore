package io.arenadata.dtm.query.execution.core.verticle;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;
import io.arenadata.dtm.query.execution.core.delta.dto.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.factory.impl.BeginDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.service.impl.BeginDeltaExecutor;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class StatusEventVerticleTest {
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(BeginDeltaQueryResultFactory.class);

    @BeforeEach
    void beforeAll() {
        delta.setDatamart(req.getDatamartMnemonic());
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult());
        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);
        DeltaServiceDao deltaServiceDao = mock(DeltaServiceDao.class);
        when(deltaServiceDao.writeNewDeltaHot(any())).thenReturn(Future.succeededFuture(0L));
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void publishDeltaOpenEvent() {
        BeginDeltaExecutor beginDeltaExecutor =
                spy(new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, null, null));
        req.setSql("BEGIN DELTA");
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart("test")
                .request(req)
                .build();
        doNothing().when(beginDeltaExecutor).publishStatus(any(), any(), any());
        when(beginDeltaExecutor.getVertx()).thenReturn(null);
        beginDeltaExecutor.execute(deltaQuery);
        verify(beginDeltaExecutor, times(1)).publishStatus(eq(StatusEventCode.DELTA_OPEN),
                eq(deltaQuery.getDatamart()), any());
    }

    private List<Map<String, Object>> createResult() {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.NUM_FIELD),
                Collections.singletonList(0));
    }

}
