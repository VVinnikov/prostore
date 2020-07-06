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
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dao.impl.ServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.*;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.impl.UploadKafkaExecutor;
import ru.ibs.dtm.query.execution.core.service.impl.CalciteDefinitionService;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

class UploadExternalTableExecutorTest {

    private final ServiceDao serviceDao = mock(ServiceDaoImpl.class);
    private final EdmlProperties edmlProperties = mock(EdmlProperties.class);
    private List<EdmlUploadExecutor> uploadExecutors = Arrays.asList(mock(UploadKafkaExecutor.class));
    private UploadExternalTableExecutor uploadExternalTableExecutor;
    private CalciteConfiguration config = new CalciteConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));
    private QueryRequest queryRequest;
    private UploadExtTableRecord uploadRecord;

    @BeforeEach
    void setUp() {
        uploadExternalTableExecutor = new UploadExternalTableExecutor(serviceDao, edmlProperties, uploadExecutors);
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId("6efad624-b9da-4ba1-9fed-f2da478b08e8");

        uploadRecord = new UploadExtTableRecord();
        uploadRecord.setId(1L);
        uploadRecord.setDatamartId(1L);
        uploadRecord.setTableName("upload_table");
        uploadRecord.setFormat(Format.AVRO);
        uploadRecord.setLocationType(Type.KAFKA_TOPIC);
        uploadRecord.setLocationPath("kafka://kafka-1.dtm.local:9092/topic");
        uploadRecord.setMessageLimit(1000);

    }

    @Test
    void executeKafkaExecutorSuccess() throws Exception {
        Promise promise = Promise.promise();
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        final DeltaRecord deltaRecord = new DeltaRecord();
        deltaRecord.setDatamartMnemonic(context.getRequest().getQueryRequest().getDatamartMnemonic());
        deltaRecord.setLoadId(1L);
        deltaRecord.setSinId(1L);
        deltaRecord.setStatus(DeltaLoadStatus.IN_PROCESS);

        EdmlQuery edmlQuery = new EdmlQuery(EdmlAction.UPLOAD, uploadRecord);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(deltaRecord));
            return null;
        }).when(serviceDao).getDeltaHotByDatamart(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(serviceDao).inserUploadQuery(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        uploadExternalTableExecutor.execute(context, edmlQuery, ar -> {
            if (ar.succeeded()){
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });

        assertEquals(context.getLoadParam().getDeltaHot(), deltaRecord.getSinId());
        assertEquals(context.getLoadParam().getTableName(), context.getTargetTable().getTableName());
        assertEquals(context.getLoadParam().getSqlQuery().replace("\n", " ").toLowerCase(), insertSql.toLowerCase());
        assertEquals(context.getLoadParam().getDatamart(), context.getRequest().getQueryRequest().getDatamartMnemonic());
        assertEquals(context.getLoadParam().getMessageLimit(), uploadRecord.getMessageLimit());
    }

    @Test
    void executeKafkaExecutorFindDeltaError() throws Exception {
        Promise promise = Promise.promise();
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        EdmlQuery edmlQuery = new EdmlQuery(EdmlAction.UPLOAD, uploadRecord);
        RuntimeException exception = new RuntimeException("Не найдена открытая дельта!");

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(exception));
            return null;
        }).when(serviceDao).getDeltaHotByDatamart(any(), any());

        uploadExternalTableExecutor.execute(context, edmlQuery, ar -> {
            if (ar.succeeded()){
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(exception, promise.future().cause());
    }

    @Test
    void executeKafkaExecutorWithDeltaIncorrectStatus() throws Exception {
        Promise promise = Promise.promise();
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        EdmlQuery edmlQuery = new EdmlQuery(EdmlAction.UPLOAD, uploadRecord);
        final DeltaRecord deltaRecord = new DeltaRecord();
        deltaRecord.setDatamartMnemonic(context.getRequest().getQueryRequest().getDatamartMnemonic());
        deltaRecord.setLoadId(1L);
        deltaRecord.setSinId(1L);
        deltaRecord.setStatus(DeltaLoadStatus.SUCCESS);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(deltaRecord));
            return null;
        }).when(serviceDao).getDeltaHotByDatamart(any(), any());

        uploadExternalTableExecutor.execute(context, edmlQuery, ar -> {
            if (ar.succeeded()){
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertNotNull(promise.future().cause());
    }
}