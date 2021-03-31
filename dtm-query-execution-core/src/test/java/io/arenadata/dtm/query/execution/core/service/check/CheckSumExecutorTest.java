package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckSum;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.exception.check.CheckSumException;
import io.arenadata.dtm.query.execution.core.exception.delta.DeltaNotFoundException;
import io.arenadata.dtm.query.execution.core.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.check.CheckQueryResultFactoryImpl;
import io.arenadata.dtm.query.execution.core.service.check.impl.CheckSumExecutor;
import io.arenadata.dtm.query.execution.core.service.check.impl.CheckSumTableServiceImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CheckSumExecutorTest {

    private final EntityDao entityDao = mock(EntityDao.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDao.class);
    private final CheckSumTableService checkSumTableService = mock(CheckSumTableServiceImpl.class);
    private final CheckQueryResultFactory queryResultFactory = new CheckQueryResultFactoryImpl();
    private CheckSumExecutor checkSumExecutor;
    private final static String DATAMART_MNEMONIC = "test";
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());
    private Entity entity;

    @BeforeEach
    void setUp() {
        checkSumExecutor = new CheckSumExecutor(deltaServiceDao, entityDao, checkSumTableService, queryResultFactory);
        entity = Entity.builder()
                .schema(DATAMART_MNEMONIC)
                .entityType(EntityType.TABLE)
                .destination(SOURCE_TYPES)
                .name("test_table")
                .fields(Arrays.asList(EntityField.builder()
                                .name("f1")
                                .build(),
                        EntityField.builder()
                                .name("f2")
                                .build(),
                        EntityField.builder()
                                .name("f3")
                                .build()))
                .build();
        when(entityDao.getEntity(DATAMART_MNEMONIC, entity.getName()))
                .thenReturn(Future.succeededFuture(entity));
    }

    @Test
    void executeNonEqualHotDelNum() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        Long hashSum = 12345L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);
        OkDelta okDelta = OkDelta.builder()
                .deltaNum(deltaNum)
                .cnFrom(0)
                .cnTo(1)
                .build();
        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));

        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, deltaNum))
                .thenReturn(Future.succeededFuture(okDelta));

        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeEqualHotDelNum() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        Long hashSum = 12345L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);

        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(0)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));

        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeNullHotDelta() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        Long hashSum = 12345L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);
        OkDelta okDelta = OkDelta.builder()
                .deltaNum(deltaNum)
                .cnFrom(0)
                .cnTo(1)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(null));

        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, deltaNum))
                .thenReturn(Future.succeededFuture(okDelta));

        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeWithGetDeltaHotError() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.failedFuture(new DtmException("")));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void executeCheckSumTableError() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);
        OkDelta okDelta = OkDelta.builder()
                .deltaNum(deltaNum)
                .cnFrom(0)
                .cnTo(1)
                .build();

        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));

        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, deltaNum))
                .thenReturn(Future.succeededFuture(okDelta));

        when(checkSumTableService.calcCheckSumTable(any()))
                .thenReturn(Future.failedFuture(new CheckSumException(entity.getName())));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals(CheckSumException.class, promise.future().cause().getClass());
    }

    @Test
    void executeNullCheckSumTableSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        Long hashSum = null;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);
        OkDelta okDelta = OkDelta.builder()
                .deltaNum(deltaNum)
                .cnFrom(0)
                .cnTo(1)
                .build();

        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));

        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, deltaNum))
                .thenReturn(Future.succeededFuture(okDelta));

        when(checkSumTableService.calcCheckSumTable(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum, promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeWithNonTableEntity() {
        Promise<QueryResult> promise = Promise.promise();
        entity.setEntityType(EntityType.VIEW);
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        Long hashSum = 12345L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);
        OkDelta okDelta = OkDelta.builder()
                .deltaNum(deltaNum)
                .cnFrom(0)
                .cnTo(1)
                .build();

        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));

        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, deltaNum))
                .thenReturn(Future.succeededFuture(okDelta));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals(EntityNotExistsException.class, promise.future().cause().getClass());
    }

    @Test
    void executeWithGetDeltaNumError() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(entity.getName());
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);

        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));

        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, deltaNum))
                .thenReturn(Future.failedFuture(new DeltaNotFoundException()));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void executeCheckSumAllTableSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        Long hashSum = 12345L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(null);
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);
        OkDelta okDelta = OkDelta.builder()
                .deltaNum(deltaNum)
                .cnFrom(0)
                .cnTo(1)
                .build();

        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));

        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, deltaNum))
                .thenReturn(Future.succeededFuture(okDelta));

        when(checkSumTableService.calcCheckSumForAllTables(any())).thenReturn(Future.succeededFuture(hashSum));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(hashSum.toString(), promise.future().result().getResult().get(0).get("check_result"));
    }

    @Test
    void executeCheckSumAllTableError() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        long deltaNum = 0L;
        SqlCheckSum sqlCheckSum = mock(SqlCheckSum.class);
        when(sqlCheckSum.getDeltaNum()).thenReturn(deltaNum);
        when(sqlCheckSum.getTable()).thenReturn(null);
        when(sqlCheckSum.getColumns()).thenReturn(null);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.SUM, sqlCheckSum);
        OkDelta okDelta = OkDelta.builder()
                .deltaNum(deltaNum)
                .cnFrom(0)
                .cnTo(1)
                .build();

        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(0L)
                .cnTo(1L)
                .build();

        when(deltaServiceDao.getDeltaHot(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(hotDelta));

        when(deltaServiceDao.getDeltaByNum(DATAMART_MNEMONIC, deltaNum))
                .thenReturn(Future.succeededFuture(okDelta));

        when(checkSumTableService.calcCheckSumForAllTables(any()))
                .thenReturn(Future.failedFuture(new CheckSumException(entity.getName())));

        checkSumExecutor.execute(checkContext)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals(CheckSumException.class, promise.future().cause().getClass());
    }

}