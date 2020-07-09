package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dao.impl.ServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;
import ru.ibs.dtm.query.execution.core.service.edml.impl.DownloadExternalTableExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import ru.ibs.dtm.query.execution.core.service.impl.EdmlServiceImpl;
import ru.ibs.dtm.query.execution.core.service.impl.SchemaStorageProviderImpl;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.EdmlService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EdmlServiceImplTest {
    private CalciteConfiguration config = new CalciteConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));
    private final SchemaStorageProvider schemaStorageProvider = mock(SchemaStorageProviderImpl.class);
    private final ServiceDao serviceDao = mock(ServiceDaoImpl.class);
    private List<EdmlExecutor> edmlExecutors = Arrays.asList(mock(DownloadExternalTableExecutor.class), mock(UploadExternalTableExecutor.class));
    private EdmlService<QueryResult> edmlService;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId("6efad624-b9da-4ba1-9fed-f2da478b08e8");
    }

    @Test
    void executeDownloadExtTableSuccess() throws Throwable {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDao, schemaStorageProvider, edmlExecutors);
        Promise promise = Promise.promise();
        JsonObject schema = new JsonObject();
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        TableInfo targetTable = new TableInfo("test", "download_table");
        TableInfo sourceTable = new TableInfo("test", "pso");

        DownloadExtTableRecord downRecord = new DownloadExtTableRecord();
        downRecord.setId(1L);
        downRecord.setDatamartId(1L);
        downRecord.setTableName("download_table");
        downRecord.setFormat(Format.AVRO);
        downRecord.setLocationType(Type.KAFKA_TOPIC);
        downRecord.setLocationPath("kafka://kafka-1.dtm.local:9092/topic");
        downRecord.setChunkSize(1000);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<JsonObject>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(schemaStorageProvider).getLogicalSchema(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DownloadExtTableRecord>> handler = invocation.getArgument(2);
            String schemaName = invocation.getArgument(0);
            String tableName = invocation.getArgument(1);
            if (context.getSourceTable().getSchemaName().equals(schemaName)
                    && context.getSourceTable().getTableName().equals(tableName)) {
                handler.handle(Future.failedFuture(new RuntimeException()));
            } else {
                handler.handle(Future.succeededFuture(downRecord));
            }
            return null;
        }).when(serviceDao).findDownloadExternalTable(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<EdmlQuery>> handler = invocation.getArgument(2);
            String schemaName = invocation.getArgument(0);
            String tableName = invocation.getArgument(1);
            if (context.getTargetTable().getSchemaName().equals(schemaName)
                    && context.getTargetTable().getTableName().equals(tableName)) {
                handler.handle(Future.failedFuture(new RuntimeException()));
            } else {
                handler.handle(Future.succeededFuture());
            }
            return null;
        }).when(serviceDao).findUploadExternalTable(any(), any(), any());


        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(edmlExecutors.get(0)).execute(any(), any(), any());

        edmlService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(context.getSourceTable(), sourceTable);
        assertEquals(context.getTargetTable(), targetTable);
        assertNotNull(promise.future().result());
    }

    @Test
    void executeUploadExtTableSuccess() throws Throwable {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDao, schemaStorageProvider, edmlExecutors);
        Promise promise = Promise.promise();
        JsonObject schema = new JsonObject();
        queryRequest.setSql("INSERT INTO test.pso SELECT id, name FROM test.upload_table");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        TableInfo targetTable = new TableInfo("test", "pso");
        TableInfo sourceTable = new TableInfo("test", "upload_table");

        UploadExtTableRecord uploadRecord = new UploadExtTableRecord();
        uploadRecord.setId(1L);
        uploadRecord.setDatamartId(1L);
        uploadRecord.setTableName("upload_table");
        uploadRecord.setFormat(Format.AVRO);
        uploadRecord.setLocationType(Type.KAFKA_TOPIC);
        uploadRecord.setLocationPath("kafka://kafka-1.dtm.local:9092/topic");
        uploadRecord.setMessageLimit(1000);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<JsonObject>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(schemaStorageProvider).getLogicalSchema(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DownloadExtTableRecord>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException()));
            return null;
        }).when(serviceDao).findDownloadExternalTable(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<UploadExtTableRecord>> handler = invocation.getArgument(2);
            String schemaName = invocation.getArgument(0);
            String tableName = invocation.getArgument(1);
            if (context.getTargetTable().getSchemaName().equals(schemaName)
                    && context.getTargetTable().getTableName().equals(tableName)) {
                handler.handle(Future.failedFuture(new RuntimeException()));
            } else {
                handler.handle(Future.succeededFuture(uploadRecord));
            }
            return null;
        }).when(serviceDao).findUploadExternalTable(any(), any(), any());


        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(edmlExecutors.get(1)).execute(any(), any(), any());

        edmlService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(context.getSourceTable(), sourceTable);
        assertEquals(context.getTargetTable(), targetTable);
        assertNotNull(promise.future().result());
    }


    @Test
    void executeDownloadExtTableAsSource() throws Throwable {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDao, schemaStorageProvider, edmlExecutors);
        Promise promise = Promise.promise();
        JsonObject schema = new JsonObject();
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<JsonObject>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(schemaStorageProvider).getLogicalSchema(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(serviceDao).findDownloadExternalTable(any(), any(), any());

        edmlService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeUploadExtTableAsTarget() throws Throwable {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDao, schemaStorageProvider, edmlExecutors);
        Promise promise = Promise.promise();
        JsonObject schema = new JsonObject();
        queryRequest.setSql("INSERT INTO test.pso SELECT id, name FROM test.upload_table");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<JsonObject>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(schemaStorageProvider).getLogicalSchema(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(new RuntimeException()));
            return null;
        }).when(serviceDao).findDownloadExternalTable(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(serviceDao).findUploadExternalTable(any(), any(), any());

        edmlService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}
