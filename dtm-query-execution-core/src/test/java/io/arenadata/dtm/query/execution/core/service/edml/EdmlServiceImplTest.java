package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.service.edml.impl.DownloadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.impl.EdmlServiceImpl;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.EdmlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EdmlServiceImplTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final List<EdmlExecutor> edmlExecutors = Arrays.asList(mock(DownloadExternalTableExecutor.class), mock(UploadExternalTableExecutor.class));
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private EdmlService<QueryResult> edmlService;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
    }

    @Test
    void executeDownloadExtTableSuccess() throws Throwable {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDbFacade, edmlExecutors);
        Promise promise = Promise.promise();

        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        TableInfo targetTable = new TableInfo("test", "download_table");
        TableInfo sourceTable = new TableInfo("test", "pso");

        Entity downloadEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity uploadEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("upload_table")
                .schema("test")
                .build();

        Mockito.when(entityDao.getEntity(eq("test"), eq("download_table"))).thenReturn(Future.succeededFuture(downloadEntity));

        Mockito.when(entityDao.getEntity(eq("test"), eq("upload_table"))).thenReturn(Future.succeededFuture(uploadEntity));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(edmlExecutors.get(0)).execute(any(), any());

        edmlService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(context.getSourceTable(), sourceTable);
        assertEquals(context.getTargetTable(), targetTable);
    }

    @Test
    void executeUploadExtTableSuccess() throws Throwable {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDbFacade, edmlExecutors);
        Promise promise = Promise.promise();
        queryRequest.setSql("INSERT INTO test.pso SELECT id, name FROM test.upload_table");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        Entity downloadEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("pso")
                .schema("test")
                .build();

        Entity uploadEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("upload_table")
                .schema("test")
                .build();

        Mockito.when(entityDao.getEntity(eq("test"), eq("pso"))).thenReturn(Future.succeededFuture(downloadEntity));

        Mockito.when(entityDao.getEntity(eq("test"), eq("upload_table"))).thenReturn(Future.succeededFuture(uploadEntity));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(edmlExecutors.get(1)).execute(any(), any());

        edmlService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().succeeded());
        assertEquals(context.getEntity(), uploadEntity);
    }

    @Test
    void executeDownloadExtTableAsSource() throws Throwable {
        when(edmlExecutors.get(0).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(1).getAction()).thenReturn(EdmlAction.UPLOAD);
        edmlService = new EdmlServiceImpl(serviceDbFacade, edmlExecutors);
        Promise promise = Promise.promise();
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);

        Entity downloadEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity uploadEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("upload_table")
                .schema("test")
                .build();

        Mockito.when(entityDao.getEntity(eq("test"), eq("download_table"))).thenReturn(Future.succeededFuture(downloadEntity));

        Mockito.when(entityDao.getEntity(eq("test"), eq("111"))).thenReturn(Future.succeededFuture(uploadEntity));

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