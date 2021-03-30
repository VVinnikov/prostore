/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.service.rollback;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.dto.edml.EraseWriteOpResult;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadFailedExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.rollback.impl.RestoreStateServiceImpl;
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