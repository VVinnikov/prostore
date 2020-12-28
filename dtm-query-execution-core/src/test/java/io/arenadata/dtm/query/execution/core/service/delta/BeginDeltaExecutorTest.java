package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.delta.query.BeginDeltaQuery;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.delta.BeginDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.impl.BeginDeltaExecutor;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class BeginDeltaExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(BeginDeltaQueryResultFactory.class);
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService = mock(CacheService.class);
    private BeginDeltaExecutor beginDeltaExecutor;
    private QueryRequest req = new QueryRequest();
    private String datamart;

    @BeforeEach
    void beforeAll() {
        datamart = "test_datamart";
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        doNothing().when(cacheService).removeIf(any());
    }

    @Test
    void executeSuccessWithoutNum() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx(),
                cacheService);
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
        checkEvictCache();
    }

    @Test
    void executeSuccessWithNum() {
        req.setSql("BEGIN DELTA SET 2");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx(),
                cacheService);
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
        checkEvictCache();
    }

    @Test
    void executeWriteNewDeltaHotError() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx(),
                cacheService);
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
        verify(cacheService, times(0)).removeIf(any());
    }

    @Test
    void executeWithNumWriteNewDeltaHotError() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx(),
                cacheService);
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
        verify(cacheService, times(0)).removeIf(any());
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx(),
                cacheService);
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
        verify(cacheService, times(0)).removeIf(any());
    }

    private List<Map<String, Object>> createResult(Long deltaNum) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.NUM_FIELD),
                Collections.singletonList(deltaNum));
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
