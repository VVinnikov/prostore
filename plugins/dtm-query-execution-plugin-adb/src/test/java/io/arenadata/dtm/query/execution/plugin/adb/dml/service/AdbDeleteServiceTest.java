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
package io.arenadata.dtm.query.execution.plugin.adb.dml.service;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.AdbDmlQueryExtendWithoutHistoryService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.AdbQueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.AdbQueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.AdbSchemaExtender;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlwUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.DeleteRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbDeleteServiceTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final QueryExtendService queryExtender = new AdbDmlQueryExtendWithoutHistoryService();
    private final AdbCalciteContextProvider contextProvider = new AdbCalciteContextProvider(
            calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()),
            new AdbCalciteSchemaFactory(new AdbSchemaFactory()));
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
    private final AdbQueryGenerator adbQueryGeneratorimpl = new AdbQueryGenerator(queryExtender, calciteConfiguration.adbSqlDialect(), relToSqlConverter);
    private final QueryEnrichmentService adbQueryEnrichmentService = new AdbQueryEnrichmentService(adbQueryGeneratorimpl, contextProvider, new AdbSchemaExtender());

    @Mock
    private DatabaseExecutor executor;

    @Mock
    private AdbMppwDataTransferService mppwTransferDataHandler;

    @Captor
    private ArgumentCaptor<String> executorArgCaptor;

    @Captor
    private ArgumentCaptor<TransferDataRequest> transferRequestCaptor;

    private AdbDeleteService adbDeleteService;

    @BeforeEach
    void setUp(Vertx vertx) {
        val queryParserService = new AdbCalciteDMLQueryParserService(contextProvider, vertx);
        adbDeleteService = new AdbDeleteService(executor, mppwTransferDataHandler, adbQueryEnrichmentService, queryParserService, calciteConfiguration.adbSqlDialect());

        lenient().when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        lenient().when(mppwTransferDataHandler.execute(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenCondition(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.failed()) {
                            fail(ar.cause());
                        }

                        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                        val executorParam = executorArgCaptor.getValue();
                        assertEquals("INSERT INTO datamart.abc_staging (id,sys_op) SELECT id, 1 AS EXPR__1 FROM (SELECT id, col1, col2 FROM datamart.abc_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 WHERE id > 0 AND (col1 = 0 AND col2 <> 0)", executorParam);

                        verify(mppwTransferDataHandler).execute(transferRequestCaptor.capture());
                        val mppwRequest = transferRequestCaptor.getValue();
                        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
                        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
                        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getColumnList());
                        assertEquals(Arrays.asList("id"), mppwRequest.getKeyColumnList());
                        assertEquals(request.getSysCn(), mppwRequest.getHotDelta());
                    }).completeNow();
                });
    }

    @Test
    void shouldSuccessWhenNotNullableFields(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", false);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.failed()) {
                            fail(ar.cause());
                        }

                        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                        val executorParam = executorArgCaptor.getValue();
                        assertEquals("INSERT INTO datamart.abc_staging (id,col2,sys_op) SELECT id, col2, 1 AS EXPR__2 FROM (SELECT id, col1, col2 FROM datamart.abc_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 WHERE id > 0 AND (col1 = 0 AND col2 <> 0)", executorParam);

                        verify(mppwTransferDataHandler).execute(transferRequestCaptor.capture());
                        val mppwRequest = transferRequestCaptor.getValue();
                        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
                        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
                        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getColumnList());
                        assertEquals(Arrays.asList("id"), mppwRequest.getKeyColumnList());
                        assertEquals(request.getSysCn(), mppwRequest.getHotDelta());
                    }).completeNow();
                });
    }

    @Test
    void shouldSuccessWhenNoCondition(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.failed()) {
                            fail(ar.cause());
                        }

                        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                        val executorParam = executorArgCaptor.getValue();
                        assertEquals("INSERT INTO datamart.abc_staging (id,sys_op) SELECT id, 1 AS EXPR__1 FROM datamart.abc_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0", executorParam);

                        verify(mppwTransferDataHandler).execute(transferRequestCaptor.capture());
                        val mppwRequest = transferRequestCaptor.getValue();
                        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
                        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
                        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getColumnList());
                        assertEquals(Arrays.asList("id"), mppwRequest.getKeyColumnList());
                        assertEquals(request.getSysCn(), mppwRequest.getHotDelta());
                    }).completeNow();
                });
    }

    @Test
    void shouldSuccessWhenConditionAndAlias(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc as a WHERE a.id > 0 AND a.col1 = 0 AND a.col2 <> 0", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.failed()) {
                            fail(ar.cause());
                        }

                        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                        val executorParam = executorArgCaptor.getValue();
                        assertEquals("INSERT INTO datamart.abc_staging (id,sys_op) SELECT id, 1 AS EXPR__1 FROM (SELECT id, col1, col2 FROM datamart.abc_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 WHERE id > 0 AND (col1 = 0 AND col2 <> 0)", executorParam);

                        verify(mppwTransferDataHandler).execute(transferRequestCaptor.capture());
                        val mppwRequest = transferRequestCaptor.getValue();
                        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
                        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
                        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getColumnList());
                        assertEquals(Arrays.asList("id"), mppwRequest.getKeyColumnList());
                        assertEquals(request.getSysCn(), mppwRequest.getHotDelta());
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenWrongQuery(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE unknown_col = 0", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenExecutorThrows(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeWithParams(anyString(), any(), any())).thenThrow(new RuntimeException("Exception"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenExecutorFails(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.failedFuture("Failed"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenTransferThrows(VertxTestContext testContext) {
        // arrange
        reset(mppwTransferDataHandler);
        when(mppwTransferDataHandler.execute(any())).thenThrow(new RuntimeException("Exception"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenTransferFails(VertxTestContext testContext) {
        // arrange
        reset(mppwTransferDataHandler);
        when(mppwTransferDataHandler.execute(any())).thenReturn(Future.failedFuture("Failed"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > 0 AND col1 = 0 AND col2 <> 0", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    private DeleteRequest getDeleteRequest(String sql, boolean nullable) {
        val sqlNode = (SqlDelete) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val entity = Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.BIGINT)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.BIGINT)
                                .nullable(nullable)
                                .build()
                ))
                .build();

        val schema = Datamart.builder()
                .mnemonic("datamart")
                .entities(singletonList(entity))
                .isDefault(true)
                .build();

        return new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L, Collections.singletonList(schema), null);
    }
}