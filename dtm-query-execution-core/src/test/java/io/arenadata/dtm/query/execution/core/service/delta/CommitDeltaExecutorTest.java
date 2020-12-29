package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.delta.CommitDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.impl.CommitDeltaExecutor;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class CommitDeltaExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(CommitDeltaQueryResultFactory.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheService.class);
    private CommitDeltaExecutor commitDeltaExecutor;
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    private final String datamart = "test_datamart";

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setDatamart(req.getDatamartMnemonic());
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
        doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString(), any());
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx(), evictQueryTemplateCacheService);
        req.setSql("COMMIT DELTA");
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));

        when(deltaServiceDao.writeDeltaHotSuccess(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaDate));

        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);

        commitDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertEquals(deltaDate, ((QueryResult) promise.future().result()).getResult()
                .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
        verifyEvictCacheExecuted();
    }

    @Test
    void executeWithDatetimeSuccess() {
        Promise promise = Promise.promise();
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx(), evictQueryTemplateCacheService);
        String deltaInputDate = "2020-06-15 14:00:11";
        req.setSql("COMMIT DELTA '" + deltaInputDate + "'");

        final LocalDateTime deltaDate = LocalDateTime.parse(deltaInputDate,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .deltaDate(deltaDate)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));

        when(deltaServiceDao.writeDeltaHotSuccess(any(), any()))
                .thenReturn(Future.succeededFuture(deltaDate));

        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);

        commitDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);

        assertEquals(deltaDate, ((QueryResult) promise.future().result()).getResult()
                .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
        verifyEvictCacheExecuted();
    }

    @Test
    void executeWriteDeltaHotSuccessError() {
        req.setSql("COMMIT DELTA");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx(), evictQueryTemplateCacheService);
        Promise promise = Promise.promise();

        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());

        RuntimeException exception = new DtmException("");

        when(deltaServiceDao.writeDeltaHotSuccess(eq(datamart)))
                .thenReturn(Future.failedFuture(exception));

        commitDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verifyEvictCacheNotExecuted();
    }

    @Test
    void executeWithDatetimeWriteDeltaHotSuccessError() {
        Promise promise = Promise.promise();
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx(), evictQueryTemplateCacheService);
        String deltaInputDate = "2020-06-12 18:00:01";
        req.setSql("COMMIT DELTA '" + deltaInputDate + "'");

        final LocalDateTime deltaDate = LocalDateTime.parse(deltaInputDate,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);

        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .deltaDate(deltaDate)
                .build();

        when(deltaServiceDao.writeDeltaHotSuccess(eq(datamart), eq(deltaDate)))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        commitDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verifyEvictCacheNotExecuted();
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        req.setSql("COMMIT DELTA");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx(),
                evictQueryTemplateCacheService);
        Promise promise = Promise.promise();
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);

        CommitDeltaQuery deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));

        when(deltaServiceDao.writeDeltaHotSuccess(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaDate));

        when(deltaQueryResultFactory.create(any()))
                .thenThrow(new DtmException(""));

        commitDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verifyEvictCacheNotExecuted();
    }

    private List<Map<String, Object>> createResult(LocalDateTime deltaDate) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.DATE_TIME_FIELD),
                Collections.singletonList(deltaDate));
    }

    private void verifyEvictCacheExecuted() {
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName(datamart);
    }

    private void verifyEvictCacheNotExecuted() {
        verify(evictQueryTemplateCacheService, times(0)).evictByDatamartName(anyString());
        verify(evictQueryTemplateCacheService, times(0)).evictByEntityName(anyString(),
                anyString(), any());
    }
}
