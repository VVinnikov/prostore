package io.arenadata.dtm.query.execution.core.verticle;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.CoreTestConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.impl.BeginDeltaExecutor;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {CoreTestConfiguration.class,
        StatusEventVerticle.class
})
class StatusEventVerticleTest {
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final Vertx vertx = mock(Vertx.class);
    @MockBean
    @Qualifier("beginDeltaQueryResultFactory")
    private DeltaQueryResultFactory deltaQueryResultFactory;

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
        req.setSql("BEGIN DELTA");
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart("test")
                .request(req)
                .build();
        BeginDeltaExecutor beginDeltaExecutor =
                spy(new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, vertx));
        doNothing().when(beginDeltaExecutor).publishStatus(any(), any(), any());
        beginDeltaExecutor.execute(deltaQuery);
        verify(beginDeltaExecutor, times(1)).publishStatus(eq(StatusEventCode.DELTA_OPEN),
                eq(deltaQuery.getDatamart()), any());
    }

    private List<Map<String, Object>> createResult() {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.NUM_FIELD),
                Collections.singletonList(0));
    }

}
