package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.model.ddl.ExternalTableLocationType;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.TableAttribute;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.transformer.Transformer;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.calcite.core.service.impl.DeltaQueryPreprocessorImpl;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadExtTableAttributeDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadQueryDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.EddlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.DownloadExtTableAttributeDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.DownloadQueryDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.EddlServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExternalTableAttribute;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.service.edml.impl.DownloadExternalTableExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.impl.DownloadKafkaExecutor;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import ru.ibs.dtm.query.execution.core.service.schema.impl.LogicalSchemaProviderImpl;
import ru.ibs.dtm.query.execution.core.transformer.DownloadExtTableAttributeTransformer;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DownloadExternalTableExecutorTest {
        private final LogicalSchemaProvider logicalSchemaProvider = mock(LogicalSchemaProviderImpl.class);
    private final DeltaQueryPreprocessor deltaQueryPreprocessor = mock(DeltaQueryPreprocessorImpl.class);
    private final List<EdmlDownloadExecutor> downloadExecutors = Arrays.asList(mock(DownloadKafkaExecutor.class));
    private DownloadExternalTableExecutor downloadExternalTableExecutor;
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private QueryRequest queryRequest;
    private Entity entity;
    private List<Datamart> schema = Collections.emptyList();

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId(UUID.randomUUID().toString());

        entity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("download_table")
                .schema("test")
                .externalTableSchema("")
                .build();
    }

    @Test
    void executeKafkaExecutorSuccess() {
        Promise promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
                deltaQueryPreprocessor, downloadExecutors);
        String selectSql = "select id, lst_nam FROM test.pso";
        String insertSql = "insert into test.download_table " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setEntity(entity);
        context.setTargetTable(new TableInfo("test", "download_table"));
        context.setSourceTable(new TableInfo("test", "pso"));

        QueryRequest copyRequest = context.getRequest().getQueryRequest();
        copyRequest.setDeltaInformations(Collections.emptyList());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());

        when(deltaQueryPreprocessor.process(any()))
                .thenReturn(Future.succeededFuture(copyRequest));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(downloadExecutors.get(0)).execute(any(), any());

        downloadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertNotNull(context.getRequest().getQueryRequest().getDeltaInformations());
    }

    @Test
    void executeKafkaGetLogicalSchemaError() {
        Promise promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
                deltaQueryPreprocessor, downloadExecutors);
        String selectSql = "select id, lst_nam FROM test.pso";
        String insertSql = "insert into test.download_table " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setEntity(entity);
        context.setTargetTable(new TableInfo("test", "download_table"));
        context.setSourceTable(new TableInfo("test", "pso"));

        QueryRequest copyRequest = context.getRequest().getQueryRequest();
        copyRequest.setDeltaInformations(Collections.emptyList());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<JsonObject>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());

        downloadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaDeltaProcessError() {
        Promise promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
                deltaQueryPreprocessor, downloadExecutors);
        String selectSql = "select id, lst_nam FROM test.pso";
        String insertSql = "insert into test.download_table " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setEntity(entity);
        context.setTargetTable(new TableInfo("test", "download_table"));
        context.setSourceTable(new TableInfo("test", "pso"));

        QueryRequest copyRequest = context.getRequest().getQueryRequest();
        copyRequest.setDeltaInformations(Collections.emptyList());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());

        when(deltaQueryPreprocessor.process(any()))
                .thenReturn(Future.failedFuture(new RuntimeException("")));

        downloadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaExecutorError() {
        Promise promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
                deltaQueryPreprocessor, downloadExecutors);
        String selectSql = "select id, lst_nam FROM test.pso";
        String insertSql = "insert into test.download_table " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setEntity(entity);
        context.setTargetTable(new TableInfo("test", "download_table"));
        context.setSourceTable(new TableInfo("test", "pso"));

        QueryRequest copyRequest = context.getRequest().getQueryRequest();
        copyRequest.setDeltaInformations(Collections.emptyList());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Datamart>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(schema));
            return null;
        }).when(logicalSchemaProvider).getSchema(any(), any());

        when(deltaQueryPreprocessor.process(any()))
                .thenReturn(Future.succeededFuture(copyRequest));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new RuntimeException("")));
            return null;
        }).when(downloadExecutors.get(0)).execute(any(), any());

        downloadExternalTableExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}
