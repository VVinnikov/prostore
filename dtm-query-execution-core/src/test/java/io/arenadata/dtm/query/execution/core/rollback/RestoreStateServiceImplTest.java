package io.arenadata.dtm.query.execution.core.rollback;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.edml.dto.EraseWriteOpResult;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.UploadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.UploadFailedExecutorImpl;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.rollback.service.impl.RestoreStateServiceImpl;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RestoreStateServiceImplTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    private final EntityDao entityDao = mock(EntityDaoImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final EdmlUploadFailedExecutor edmlUploadFailedExecutor = mock(UploadFailedExecutorImpl.class);
    private final UploadExternalTableExecutor uploadExternalTableExecutor = mock(UploadExternalTableExecutor.class);
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private RestoreStateService restoreStateService;
    private final String envName = "test";
    private final DtmConfig dtmConfig = new CoreDtmSettings(ZoneId.of("UTC"));

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);

        restoreStateService = new RestoreStateServiceImpl(serviceDbFacade,
                edmlUploadFailedExecutor,
                uploadExternalTableExecutor,
                definitionService,
                envName,
                dtmConfig);
    }

    @Test
    void restoreStateSuccess() {
        Promise<Void> promise = Promise.promise();
        List<String> datamarts = Arrays.asList("test1", "test2", "test3");
        List<DeltaWriteOp> writeOps1 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(0)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(2)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(2L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(0)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(1L)
                        .build());
        List<DeltaWriteOp> writeOps2 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("d1")
                        .tableNameExt("d1_ext")
                        .status(0)
                        .query("insert into d1 select * from d1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("d1")
                        .tableNameExt("d1_ext")
                        .status(2)
                        .query("insert into d1 select * from d1_ext")
                        .sysCn(2L)
                        .build());

        List<DeltaWriteOp> writeOps3 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("l1")
                        .tableNameExt("l1_ext")
                        .status(2)
                        .query("insert into l1 select * from l1_ext")
                        .sysCn(2L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("l2")
                        .tableNameExt("l2_ext")
                        .status(2)
                        .query("insert into l2 select * from l2_ext")
                        .sysCn(2L)
                        .build());

        when(datamartDao.getDatamarts()).thenReturn(Future.succeededFuture(datamarts));

        when(deltaServiceDao.getDeltaWriteOperations(datamarts.get(0))).thenReturn(Future.succeededFuture(writeOps1));
        when(deltaServiceDao.getDeltaWriteOperations(datamarts.get(1))).thenReturn(Future.succeededFuture(writeOps2));
        when(deltaServiceDao.getDeltaWriteOperations(datamarts.get(2))).thenReturn(Future.succeededFuture(writeOps3));

        doAnswer(invocation -> {
            final String datamart = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(datamart).build());
        }).when(entityDao).getEntity(any(), any());

        when(uploadExternalTableExecutor.execute(any())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        when(edmlUploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        restoreStateService.restoreState()
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void restoreStateUploadError() {
        Promise<Void> promise = Promise.promise();
        List<String> datamarts = Collections.singletonList("test1");
        List<DeltaWriteOp> writeOps1 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(0)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(2)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(2L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(0)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(1L)
                        .build());

        when(datamartDao.getDatamarts()).thenReturn(Future.succeededFuture(datamarts));

        when(deltaServiceDao.getDeltaWriteOperations(datamarts.get(0))).thenReturn(Future.succeededFuture(writeOps1));

        doAnswer(invocation -> {
            final String datamart = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(datamart).build());
        }).when(entityDao).getEntity(any(), any());

        when(uploadExternalTableExecutor.execute(any())).thenReturn(Future.failedFuture(new DtmException("")));

        when(edmlUploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        assertThrows(DtmException.class, () -> restoreStateService.restoreState()
                .onComplete(promise));
    }

    @Test
    void restoreStateEraseError() {
        Promise<Void> promise = Promise.promise();
        List<String> datamarts = Collections.singletonList("test1");
        List<DeltaWriteOp> writeOps1 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(0)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(2)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(2L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(0)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(1L)
                        .build());

        when(datamartDao.getDatamarts()).thenReturn(Future.succeededFuture(datamarts));

        when(deltaServiceDao.getDeltaWriteOperations(datamarts.get(0))).thenReturn(Future.succeededFuture(writeOps1));

        doAnswer(invocation -> {
            final String datamart = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(datamart).build());
        }).when(entityDao).getEntity(any(), any());

        when(uploadExternalTableExecutor.execute(any())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        when(edmlUploadFailedExecutor.execute(any())).thenReturn(Future.failedFuture(new DtmException("")));

        assertThrows(DtmException.class, () -> restoreStateService.restoreState()
                .onComplete(promise));
    }

    @Test
    void restoreEraseSuccess() {
        Promise<List<EraseWriteOpResult>> promise = Promise.promise();
        String datamart = "test";
        List<DeltaWriteOp> writeOps1 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(2)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(2)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(1)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(2L)
                        .build());
        List<EraseWriteOpResult> eraseWriteOpResults = Arrays.asList(new EraseWriteOpResult("t1", 1),
                new EraseWriteOpResult("t2", 1));

        when(deltaServiceDao.getDeltaWriteOperations(datamart)).thenReturn(Future.succeededFuture(writeOps1));

        doAnswer(invocation -> {
            final String schema = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(schema).build());
        }).when(entityDao).getEntity(any(), any());

        when(edmlUploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        restoreStateService.restoreErase(datamart)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(eraseWriteOpResults, promise.future().result());
    }

    @Test
    void restoreEraseEmptySuccess() {
        Promise<List<EraseWriteOpResult>> promise = Promise.promise();
        String datamart = "test";
        List<DeltaWriteOp> writeOps1 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(1)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(1)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(1)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(2L)
                        .build());

        when(deltaServiceDao.getDeltaWriteOperations(datamart)).thenReturn(Future.succeededFuture(writeOps1));

        doAnswer(invocation -> {
            final String schema = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(schema).build());
        }).when(entityDao).getEntity(any(), any());

        when(edmlUploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        restoreStateService.restoreErase(datamart)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertTrue(promise.future().result().isEmpty());
    }

    @Test
    void restoreEraseWriteOpsNullSuccess() {
        Promise<List<EraseWriteOpResult>> promise = Promise.promise();
        String datamart = "test";

        when(deltaServiceDao.getDeltaWriteOperations(datamart)).thenReturn(Future.succeededFuture(null));

        doAnswer(invocation -> {
            final String schema = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(schema).build());
        }).when(entityDao).getEntity(any(), any());

        when(edmlUploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        restoreStateService.restoreErase(datamart)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertTrue(promise.future().result().isEmpty());
    }

    @Test
    void restoreEraseError() {
        Promise<List<EraseWriteOpResult>> promise = Promise.promise();
        String datamart = "test";
        List<DeltaWriteOp> writeOps1 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(2)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(2)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(2)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(2L)
                        .build());

        when(deltaServiceDao.getDeltaWriteOperations(datamart)).thenReturn(Future.succeededFuture(writeOps1));

        doAnswer(invocation -> {
            final String schema = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(schema).build());
        }).when(entityDao).getEntity(any(), any());

        when(edmlUploadFailedExecutor.execute(any())).thenReturn(Future.failedFuture(new DtmException("")));

        restoreStateService.restoreErase(datamart)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void restoreUploadSuccess() {
        Promise<Void> promise = Promise.promise();
        String datamart = "test";
        List<DeltaWriteOp> writeOps1 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(0)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(0)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(2)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(2L)
                        .build());

        when(deltaServiceDao.getDeltaWriteOperations(datamart)).thenReturn(Future.succeededFuture(writeOps1));

        doAnswer(invocation -> {
            final String schema = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(schema).build());
        }).when(entityDao).getEntity(any(), any());

        when(uploadExternalTableExecutor.execute(any())).thenReturn(Future.succeededFuture());

        restoreStateService.restoreUpload(datamart)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void restoreUploadWriteOpsNullSuccess() {
        Promise<Void> promise = Promise.promise();
        String datamart = "test";

        when(deltaServiceDao.getDeltaWriteOperations(datamart)).thenReturn(Future.succeededFuture(null));

        doAnswer(invocation -> {
            final String schema = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(schema).build());
        }).when(entityDao).getEntity(any(), any());

        when(uploadExternalTableExecutor.execute(any())).thenReturn(Future.succeededFuture());

        restoreStateService.restoreUpload(datamart)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void restoreUploadError() {
        Promise<Void> promise = Promise.promise();
        String datamart = "test";
        List<DeltaWriteOp> writeOps1 = Arrays.asList(
                DeltaWriteOp.builder()
                        .tableName("t1")
                        .tableNameExt("t1_ext")
                        .status(0)
                        .query("insert into t1 select * from t1_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(0)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(1L)
                        .build(),
                DeltaWriteOp.builder()
                        .tableName("t2")
                        .tableNameExt("t2_ext")
                        .status(2)
                        .query("insert into t2 select * from t2_ext")
                        .sysCn(2L)
                        .build());

        when(deltaServiceDao.getDeltaWriteOperations(datamart)).thenReturn(Future.succeededFuture(writeOps1));

        doAnswer(invocation -> {
            final String schema = invocation.getArgument(0);
            final String tableName = invocation.getArgument(1);
            return Future.succeededFuture(Entity.builder().name(tableName).schema(schema).build());
        }).when(entityDao).getEntity(any(), any());

        when(uploadExternalTableExecutor.execute(any())).thenReturn(Future.failedFuture(new DtmException("")));

        restoreStateService.restoreUpload(datamart)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }
}