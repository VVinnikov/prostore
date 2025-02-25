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
package io.arenadata.dtm.query.execution.plugin.adp.dml;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adp.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.dto.AdpTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.transfer.AdpTransferDataService;
import io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertRequest;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdpUpsertServiceTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlDialect sqlDialect = calciteConfiguration.adpSqlDialect();

    @Mock
    private DatabaseExecutor executor;

    @Mock
    private AdpTransferDataService mppwTransferDataHandler;

    @Captor
    private ArgumentCaptor<String> executorArgCaptor;

    @Captor
    private ArgumentCaptor<AdpTransferDataRequest> transferRequestCaptor;

    private AdpUpsertService adpUpsertService;

    @BeforeEach
    void setUp() {
        adpUpsertService = new AdpUpsertService(sqlDialect, executor, mppwTransferDataHandler);

        lenient().when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        lenient().when(mppwTransferDataHandler.transferData(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenUpsertWithColumns() {
        // arrange
        val request = getUpsertRequest("UPSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        val result = adpUpsertService.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());

        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
        val executorParam = executorArgCaptor.getValue();
        Assertions.assertThat(executorParam).isEqualToIgnoringNewLines("INSERT INTO datamart.abc_staging (id, col1, col2, sys_op)\n" +
                "VALUES  (1, 2, 3, 0),\n" +
                " (1, 2, 3, 0),\n" +
                " (1, 3, 3, 0)");

        verify(mppwTransferDataHandler).transferData(transferRequestCaptor.capture());
        val mppwRequest = transferRequestCaptor.getValue();
        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getAllFields());
        assertEquals(Arrays.asList("id"), mppwRequest.getPrimaryKeys());
        assertEquals(request.getSysCn(), mppwRequest.getSysCn());
    }

    @Test
    void shouldSuccessWhenUpsertWithoutColumns() {
        // arrange
        val request = getUpsertRequest("UPSERT INTO a.abc VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        val result = adpUpsertService.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());

        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
        val executorParam = executorArgCaptor.getValue();
        Assertions.assertThat(executorParam).isEqualToIgnoringNewLines("INSERT INTO datamart.abc_staging (id, col1, col2, sys_op)\n" +
                "VALUES  (1, 2, 3, 0),\n" +
                " (1, 2, 3, 0),\n" +
                " (1, 3, 3, 0)");

        verify(mppwTransferDataHandler).transferData(transferRequestCaptor.capture());
        val mppwRequest = transferRequestCaptor.getValue();
        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getAllFields());
        assertEquals(Arrays.asList("id"), mppwRequest.getPrimaryKeys());
        assertEquals(request.getSysCn(), mppwRequest.getSysCn());
    }

    @Test
    void shouldFailWhenUnknownColumn() {
        // arrange
        val request = getUpsertRequest("UPSERT INTO a.abc(unknown_col) VALUES (1)");

        // act
        val result = adpUpsertService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("UnexpectedSuccess");
        }
        assertEquals("Column [unknown_col] not exists", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenExecutorThrows() {
        // arrange
        reset(executor);
        when(executor.executeWithParams(anyString(), any(), any())).thenThrow(new RuntimeException("Exception"));
        val request = getUpsertRequest("UPSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        val result = adpUpsertService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenExecutorFails() {
        // arrange
        reset(executor);
        when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.failedFuture("Failed"));
        val request = getUpsertRequest("UPSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        val result = adpUpsertService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenTransferThrows() {
        // arrange
        reset(mppwTransferDataHandler);
        when(mppwTransferDataHandler.transferData(any())).thenThrow(new RuntimeException("Exception"));
        val request = getUpsertRequest("UPSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        val result = adpUpsertService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenTransferFails() {
        // arrange
        reset(mppwTransferDataHandler);
        when(mppwTransferDataHandler.transferData(any())).thenReturn(Future.failedFuture("Failed"));
        val request = getUpsertRequest("UPSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        val result = adpUpsertService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenNotValuesSource() {
        // arrange
        val request = getUpsertRequest("UPSERT INTO a.abc(id,col1,col2) SELECT * FROM TBL");

        // act
        val result = adpUpsertService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    private UpsertRequest getUpsertRequest(String s) {
        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery(s);
        val entity = getEntity();

        return new UpsertRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);
    }

    private Entity getEntity() {
        return Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.BIGINT)
                                .build()
                ))
                .build();
    }

}