package io.arenadata.dtm.query.execution.core.service.edml;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadFailedExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadKafkaExecutor;
import io.arenadata.dtm.query.execution.core.calcite.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.service.datasource.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.service.schema.impl.LogicalSchemaProviderImpl;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class UploadExternalTableExecutorTest {

    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final List<EdmlUploadExecutor> uploadExecutors = Collections.singletonList(mock(UploadKafkaExecutor.class));
    private final EdmlUploadFailedExecutor uploadFailedExecutor = mock(UploadFailedExecutorImpl.class);
    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final LogicalSchemaProvider logicalSchemaProvider = mock(LogicalSchemaProviderImpl.class);
    private UploadExternalTableExecutor uploadExternalTableExecutor;
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService = mock(CacheService.class);
    private QueryRequest queryRequest;
    private Set<SourceType> sourceTypes = new HashSet<>();
    private Entity sourceEntity;
    private Entity destEntity;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        sourceTypes.addAll(Arrays.asList(SourceType.ADB, SourceType.ADG));
        sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat("avro")
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
        destEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test")
                .destination(sourceTypes)
                .build();
        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
        when(logicalSchemaProvider.getSchemaFromQuery(any())).thenReturn(Future.succeededFuture(Collections.EMPTY_LIST));
        doNothing().when(cacheService).removeIf(any());
    }

    @Test
    void executeKafkaSuccessWithSysCnExists() {
        Promise<QueryResult> promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider, cacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(sysCn);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadExecutors.get(0).execute(any()))
                .thenReturn(Future.succeededFuture(queryResult));

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.succeededFuture());

        uploadExternalTableExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(queryResult, promise.future().result());
        checkEvictCache();
    }

    @Test
    void executeKafkaSuccessWithoutSysCn() {
        Promise<QueryResult> promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider, cacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 2L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadExecutors.get(0).execute(any()))
                .thenReturn(Future.succeededFuture(queryResult));

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.succeededFuture());

        uploadExternalTableExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(queryResult, promise.future().result());
        assertEquals(context.getSysCn(), sysCn);
        checkEvictCache();
    }

    @Test
    void executeWriteNewOpError() {
        Promise<QueryResult> promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider, cacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        uploadExternalTableExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        verify(cacheService, times(0)).removeIf(any());
    }

    @Test
    void executeWriteOpSuccessError() {
        Promise<QueryResult> promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider, cacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadExecutors.get(0).execute(any()))
                .thenReturn(Future.succeededFuture(queryResult));

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.failedFuture(new DtmException("")));

        uploadExternalTableExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        verify(cacheService, times(0)).removeIf(any());
    }

    @Test
    void executeKafkaError() {
        Promise<QueryResult> promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider, cacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadExecutors.get(0).execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        when(uploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.succeededFuture());

        when(uploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        uploadExternalTableExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        verify(cacheService, times(0)).removeIf(any());
    }

    @Test
    void executeWriteOpError() {
        Promise<QueryResult> promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider, cacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadExecutors.get(0).execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.failedFuture(new DtmException("")));

        uploadExternalTableExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        verify(cacheService, times(0)).removeIf(any());
    }

    @Test
    void executeUploadFailedError() {
        Promise<QueryResult> promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider, cacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadExecutors.get(0).execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.succeededFuture());

        when(uploadFailedExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        uploadExternalTableExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        verify(cacheService, times(0)).removeIf(any());
    }

    @Test
    void executeWithNonexistingDestSource() {
        Promise<QueryResult> promise = Promise.promise();
        when(uploadExecutors.get(0).getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadExternalTableExecutor = new UploadExternalTableExecutor(deltaServiceDao,
                uploadFailedExecutor, uploadExecutors, pluginService, logicalSchemaProvider, cacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.getDestinationEntity().setDestination(new HashSet<>(Arrays.asList(SourceType.ADB,
                SourceType.ADG, SourceType.ADQM)));

        uploadExternalTableExecutor.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        verify(cacheService, times(0)).removeIf(any());
    }

    private void checkEvictCache() {
        String dest_template = "dest_template";
        List<QueryTemplateKey> cacheMap = Arrays.asList(
                QueryTemplateKey
                        .builder()
                        .sourceQueryTemplate(dest_template)
                        .logicalSchema(Collections.singletonList(new Datamart(destEntity.getSchema(), false,
                                Collections.singletonList(destEntity))))
                        .build(),
                QueryTemplateKey
                        .builder()
                        .sourceQueryTemplate("source_template")
                        .logicalSchema(Collections.singletonList(new Datamart(sourceEntity.getSchema(), false,
                                Collections.singletonList(sourceEntity))))
                        .build()
        );
        ArgumentCaptor<Predicate<QueryTemplateKey>> captor = ArgumentCaptor.forClass(Predicate.class);
        verify(cacheService, times(1)).removeIf(captor.capture());
        assertTrue(cacheMap.stream()
                .filter(captor.getValue())
                .allMatch(template -> dest_template.equals(template.getSourceQueryTemplate())));
    }
}
