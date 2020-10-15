package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.model.ddl.ExternalTableLocationType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.EddlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.UploadQueryDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.EddlServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.UploadQueryDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.impl.UploadKafkaExecutor;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UploadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final List<EdmlUploadExecutor> uploadExecutors = Arrays.asList(mock(UploadKafkaExecutor.class));
    private UploadExternalTableExecutor uploadExternalTableExecutor;
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private QueryRequest queryRequest;
    private Entity entity;

    @BeforeEach
    void setUp() {

        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));

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
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
    }

    @Test
    void executeKafkaExecutorSuccess() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao, uploadExecutors);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        context.setEntity(entity);

        Mockito.when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));
      
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryResult));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        Mockito.when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.succeededFuture());

        uploadExternalTableExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });

        assertTrue(promise.future().succeeded());
        assertEquals(queryResult, promise.future().result());
    }

    @Test
    void executeKafkaWriteNewOpError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao, uploadExecutors);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        context.setEntity(entity);

        Mockito.when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadExternalTableExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaWriteOpSuccessError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao, uploadExecutors);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        context.setEntity(entity);

        Mockito.when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryResult));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        Mockito.when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadExternalTableExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaExecutionError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao, uploadExecutors);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        context.setEntity(entity);

        Mockito.when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn))).thenReturn(Future.succeededFuture());

        when(deltaServiceDao.deleteWriteOperation(eq("test"), eq(sysCn))).thenReturn(Future.succeededFuture());

        uploadExternalTableExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaExecutionErrorAndWriteOpError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao, uploadExecutors);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        context.setEntity(entity);

        Mockito.when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadExternalTableExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
  
    @Test
    void executeKafkaExecutionAndWriteOpError() {
        Promise promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao, uploadExecutors);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        context.setEntity(entity);

        Mockito.when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn))).thenReturn(Future.succeededFuture());

        when(deltaServiceDao.deleteWriteOperation(eq("test"), eq(sysCn))).thenReturn(Future.failedFuture(new RuntimeException("")));

        uploadExternalTableExecutor.execute(context, null, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

}