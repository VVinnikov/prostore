package ru.ibs.dtm.query.execution.plugin.adb.service.impl.rollback;

import io.reactiverse.pgclient.Tuple;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.model.ddl.ExternalTableLocationType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.factory.RollbackRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.impl.RollbackRequestFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import ru.ibs.dtm.query.execution.plugin.api.request.RollbackRequest;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;

import java.util.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdbRollbackServiceTest {

    private final RollbackRequestFactory rollbackRequestFactory = mock(RollbackRequestFactoryImpl.class);
    private final AdbQueryExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private AdbRollbackService adbRollbackService;
    private Entity entity;

    @BeforeEach
    void setUp() {
        adbRollbackService = new AdbRollbackService(rollbackRequestFactory, adbQueryExecutor);
        entity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        RollbackRequest rollbackRequest = RollbackRequest.builder()
                .datamart("test")
                .sysCn(1L)
                .queryRequest(new QueryRequest())
                .targetTable("test_table")
                .entity(entity)
                .build();
        RollbackRequestContext context = new RollbackRequestContext(rollbackRequest);

        List<PreparedStatementRequest> sqlList = Arrays.asList(
                new PreparedStatementRequest("truncateSql", Tuple.tuple()),
                new PreparedStatementRequest("deleteFromActualSql", Tuple.tuple()),
                new PreparedStatementRequest("insertSql", Tuple.tuple()),
                new PreparedStatementRequest("deleteFromHistorySql", Tuple.tuple())
        );
        when(rollbackRequestFactory.create(any())).thenReturn(sqlList);
        Map<String, Integer> execCount = new HashMap<>();
        sqlList.stream().mapToInt(request -> execCount.put(request.getSql(), 0));
        List<Map<String, Object>> resultSet = new ArrayList<>();

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Map<String, Object>>>> handler = invocation.getArgument(2);
            final String sql = invocation.getArgument(0);
            execCount.put(sql, 1);
            handler.handle(Future.succeededFuture(resultSet));
            return null;
        }).when(adbQueryExecutor).execute(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Map<String, Object>>>> handler = invocation.getArgument(1);
            final List<PreparedStatementRequest> requests = invocation.getArgument(0);
            requests.forEach(r -> execCount.put(r.getSql(), 1));
            handler.handle(Future.succeededFuture(resultSet));
            return null;
        }).when(adbQueryExecutor).executeInTransaction(any(), any());

        adbRollbackService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(execCount.get(sqlList.get(0).getSql()), 1);
        assertEquals(execCount.get(sqlList.get(1).getSql()), 1);
        assertEquals(execCount.get(sqlList.get(2).getSql()), 1);
        assertEquals(execCount.get(sqlList.get(3).getSql()), 1);
    }

    @Test
    void executeError() {
        Promise promise = Promise.promise();
        RollbackRequest rollbackRequest = RollbackRequest.builder()
                .datamart("test")
                .sysCn(1L)
                .queryRequest(new QueryRequest())
                .targetTable("test_table")
                .entity(entity)
                .build();
        RollbackRequestContext context = new RollbackRequestContext(rollbackRequest);

        List<PreparedStatementRequest> sqlList = Arrays.asList(
                new PreparedStatementRequest("truncateSql", Tuple.tuple()),
                new PreparedStatementRequest("deleteFromActualSql", Tuple.tuple()),
                new PreparedStatementRequest("insertSql", Tuple.tuple()),
                new PreparedStatementRequest("deleteFromHistory", Tuple.tuple())
        );
        when(rollbackRequestFactory.create(any())).thenReturn(sqlList);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Map<String, Object>>>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(adbQueryExecutor).execute(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Map<String, Object>>>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(adbQueryExecutor).executeInTransaction(any(), any());

        adbRollbackService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}