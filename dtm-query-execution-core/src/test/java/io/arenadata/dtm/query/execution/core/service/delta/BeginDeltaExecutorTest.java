package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheServiceImpl;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.delta.dto.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.factory.impl.BeginDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.service.impl.BeginDeltaExecutor;
import io.arenadata.dtm.query.execution.core.delta.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class BeginDeltaExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(BeginDeltaQueryResultFactory.class);
    private final EvictQueryTemplateCacheServiceImpl evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheServiceImpl.class);
    private BeginDeltaExecutor beginDeltaExecutor;
    private final QueryRequest req = new QueryRequest();
    private String datamart;

    @BeforeEach
    void beforeAll() {
        datamart = "test_datamart";
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx(),
                evictQueryTemplateCacheService);
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
    }

    @Test
    void executeSuccessWithoutNum() {
        req.setSql("BEGIN DELTA");
        Promise<QueryResult> promise = Promise.promise();
        long deltaNum = 1L;
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart(datamart)
                .request(req)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum));

        when(deltaServiceDao.writeNewDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaNum));

        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);

        beginDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);

        assertEquals(deltaNum, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.NUM_FIELD));
        verifyEvictCacheExecuted();
    }

    @Test
    void executeSuccessWithNum() {
        req.setSql("BEGIN DELTA SET 2");
        Promise<QueryResult> promise = Promise.promise();
        final long deltaNum = 2L;
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart(datamart)
                .deltaNum(deltaNum)
                .request(req)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum));

        when(deltaServiceDao.writeNewDeltaHot(eq(datamart), eq(deltaNum)))
                .thenReturn(Future.succeededFuture(deltaNum));

        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);

        beginDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);

        assertEquals(deltaNum, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.NUM_FIELD));
        verifyEvictCacheExecuted();
    }

    @Test
    void executeWriteNewDeltaHotError() {
        req.setSql("BEGIN DELTA");
        Promise<QueryResult> promise = Promise.promise();

        final long deltaNum = 2L;
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart(datamart)
                .request(req)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum));

        RuntimeException exception = new DtmException("write new delta hot error");

        when(deltaServiceDao.writeNewDeltaHot(eq(datamart)))
                .thenReturn(Future.failedFuture(exception));

        beginDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertEquals(exception, promise.future().cause());
        verifyEvictCacheExecuted();
    }

    @Test
    void executeWithNumWriteNewDeltaHotError() {
        req.setSql("BEGIN DELTA");
        Promise<QueryResult> promise = Promise.promise();

        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart(datamart)
                .request(req)
                .build();

        when(deltaServiceDao.writeNewDeltaHot(eq(datamart)))
                .thenReturn(Future.failedFuture(new DtmException("")));

        beginDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verifyEvictCacheExecuted();
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        req.setSql("BEGIN DELTA");
        Promise<QueryResult> promise = Promise.promise();

        final long deltaNum = 2L;
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart(datamart)
                .request(req)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum));

        Mockito.when(deltaServiceDao.writeNewDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaNum));

        when(deltaQueryResultFactory.create(any()))
                .thenThrow(new DtmException(""));

        beginDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        verifyEvictCacheExecuted();
    }

    private List<Map<String, Object>> createResult(Long deltaNum) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.NUM_FIELD),
                Collections.singletonList(deltaNum));
    }

    private void verifyEvictCacheExecuted() {
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName(datamart);
    }
}
