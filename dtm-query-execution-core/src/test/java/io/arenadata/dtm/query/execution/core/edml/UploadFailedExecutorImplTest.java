package io.arenadata.dtm.query.execution.core.edml;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.CrashException;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.core.rollback.factory.impl.RollbackRequestContextFactoryImpl;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.UploadFailedExecutorImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UploadFailedExecutorImplTest {

    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService = mock(EvictQueryTemplateCacheService.class);
    private final RollbackRequestContextFactory rollbackRequestContextFactory = mock(RollbackRequestContextFactoryImpl.class);
    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private EdmlUploadFailedExecutor uploadFailedExecutor;
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
                .externalTableFormat(ExternalTableFormat.AVRO)
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
    }

    @Test
    void executeSuccess() {
        Promise<Void> promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutorImpl(deltaServiceDao,
                rollbackRequestContextFactory, pluginService, evictQueryTemplateCacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);

        final RollbackRequestContext rollbackRequestContext = new RollbackRequestContext(
                new RequestMetrics(),
                "test",
                RollbackRequest.builder()
                        .queryRequest(context.getRequest().getQueryRequest())
                        .datamart(context.getSourceEntity().getName())
                        .destinationTable(context.getDestinationEntity().getName())
                        .sysCn(context.getSysCn())
                        .entity(context.getDestinationEntity())
                        .build(),
                mock(SqlNode.class)
        );

        when(rollbackRequestContextFactory.create(any()))
                .thenReturn(rollbackRequestContext);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);

        when(pluginService.rollback(any(), any(), any()))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.deleteWriteOperation(eq(sourceEntity.getSchema()), eq(context.getSysCn())))
                .thenReturn(Future.succeededFuture());

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executePluginRollbackError() {
        Promise<Void> promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutorImpl(deltaServiceDao,
                rollbackRequestContextFactory, pluginService, evictQueryTemplateCacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);
        final RollbackRequestContext rollbackRequestContext = new RollbackRequestContext(
                new RequestMetrics(),
                "test",
                RollbackRequest.builder()
                        .queryRequest(context.getRequest().getQueryRequest())
                        .datamart(context.getSourceEntity().getName())
                        .destinationTable(context.getDestinationEntity().getName())
                        .sysCn(context.getSysCn())
                        .entity(context.getDestinationEntity())
                        .build(),
                mock(SqlNode.class)
                );

        when(rollbackRequestContextFactory.create(any()))
                .thenReturn(rollbackRequestContext);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);

        when(pluginService.rollback(any(), any(), any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof CrashException);
    }

    @Test
    void executeDeleteOperationError() {
        Promise<Void> promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutorImpl(deltaServiceDao,
                rollbackRequestContextFactory, pluginService, evictQueryTemplateCacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);

        when(pluginService.rollback(any(), any(), any()))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.deleteWriteOperation(eq(sourceEntity.getSchema()), eq(context.getSysCn())))
                .thenReturn(Future.failedFuture(new DtmException("")));

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }
}
