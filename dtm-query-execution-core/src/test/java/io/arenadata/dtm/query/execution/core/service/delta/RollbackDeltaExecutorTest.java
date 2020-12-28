package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
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
import io.arenadata.dtm.query.execution.core.dto.delta.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.delta.query.RollbackDeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.delta.CommitDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.impl.CommitDeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.impl.RollbackDeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RollbackDeltaExecutorTest {
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(CommitDeltaQueryResultFactory.class);
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService = mock(CacheService.class);
    private final EdmlUploadFailedExecutor edmlUploadFailedExecutor = mock(EdmlUploadFailedExecutor.class);
    private RollbackDeltaExecutor rollbackDeltaExecutor;
    private QueryRequest req = new QueryRequest();
    private DeltaRecord delta = new DeltaRecord();
    private String datamart = "test_datamart";
    private Entity entity;

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setDatamart(req.getDatamartMnemonic());
        entity = Entity.builder()
                .name("test_entity")
                .schema(datamart)
                .fields(Collections.emptyList())
                .build();
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(entityDao.getEntity(eq(datamart), any())).thenReturn(Future.succeededFuture(entity));
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(edmlUploadFailedExecutor.eraseWriteOp(any())).thenReturn(Future.succeededFuture());
        doNothing().when(cacheService).removeIf(any());
        rollbackDeltaExecutor = new RollbackDeltaExecutor(edmlUploadFailedExecutor, serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx(), cacheService);
        when(deltaServiceDao.writeDeltaError(eq(datamart), eq(null)))
                .thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture());
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
        checkEvictCache();
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
        verify(cacheService, times(0)).removeIf(any());
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
        verify(cacheService, times(0)).removeIf(any());
    }

    private List<Map<String, Object>> createResult(LocalDateTime deltaDate) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.DATE_TIME_FIELD),
                Collections.singletonList(deltaDate));
    }

    private void checkEvictCache() {
        String testTemplate = "test_template";
        List<QueryTemplateKey> cacheMap = Arrays.asList(
                QueryTemplateKey
                        .builder()
                        .sourceQueryTemplate(testTemplate)
                        .logicalSchema(Collections.singletonList(new Datamart(datamart, false,
                                Collections.emptyList())))
                        .build(),
                QueryTemplateKey
                        .builder()
                        .sourceQueryTemplate("not_used_template")
                        .logicalSchema(Collections.singletonList(new Datamart("not_used_datamart", false,
                                Collections.emptyList())))
                        .build()
        );
        ArgumentCaptor<Predicate<QueryTemplateKey>> captor = ArgumentCaptor.forClass(Predicate.class);
        verify(cacheService, times(1)).removeIf(captor.capture());
        assertEquals(1, cacheMap.stream()
                .filter(captor.getValue())
                .filter(template -> testTemplate.equals(template.getSourceQueryTemplate()))
                .count());
    }
}
