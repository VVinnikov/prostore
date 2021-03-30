package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheServiceImpl;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.query.RollbackDeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.delta.CommitDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.impl.RollbackDeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

class RollbackDeltaExecutorTest {
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(CommitDeltaQueryResultFactory.class);
    private final EdmlUploadFailedExecutor edmlUploadFailedExecutor = mock(EdmlUploadFailedExecutor.class);
    private final EvictQueryTemplateCacheServiceImpl evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheServiceImpl.class);
    private RollbackDeltaExecutor rollbackDeltaExecutor;
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    private final String datamart = "test_datamart";

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setDatamart(req.getDatamartMnemonic());
        Entity entity = Entity.builder()
                .name("test_entity")
                .schema(datamart)
                .fields(Collections.emptyList())
                .build();
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(entityDao.getEntity(eq(datamart), any())).thenReturn(Future.succeededFuture(entity));
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(edmlUploadFailedExecutor.eraseWriteOp(any())).thenReturn(Future.succeededFuture());
        rollbackDeltaExecutor = new RollbackDeltaExecutor(edmlUploadFailedExecutor, serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx(), evictQueryTemplateCacheService);
        when(deltaServiceDao.writeDeltaError(eq(datamart), eq(null)))
                .thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture());
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        req.setSql("ROLLBACK DELTA");
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        RollbackDeltaQuery deltaQuery = RollbackDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));
        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(1)
                .cnTo(3L)
                .build();
        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);
        rollbackDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertEquals(deltaDate, ((QueryResult) promise.future().result()).getResult()
                .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
        verifyEvictCacheExecuted();
    }

    @Test
    void executHotDeltaNotExistError() {
        Promise promise = Promise.promise();
        req.setSql("ROLLBACK DELTA");
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        RollbackDeltaQuery deltaQuery = RollbackDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));
        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture());
        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);
        rollbackDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verifyEvictCacheExecuted();
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        Promise promise = Promise.promise();
        req.setSql("ROLLBACK DELTA");
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        RollbackDeltaQuery deltaQuery = RollbackDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));
        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture());
        when(deltaQueryResultFactory.create(any())).thenThrow(new DtmException(""));
        rollbackDeltaExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verifyEvictCacheExecuted();
    }

    private List<Map<String, Object>> createResult(LocalDateTime deltaDate) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.DATE_TIME_FIELD),
                Collections.singletonList(deltaDate));
    }

    private void verifyEvictCacheExecuted() {
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName(datamart);
    }
}
