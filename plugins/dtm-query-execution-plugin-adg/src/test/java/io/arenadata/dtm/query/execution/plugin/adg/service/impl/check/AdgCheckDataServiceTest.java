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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class AdgCheckDataServiceTest {
    private final static Long RESULT = 1L;
    private final AdgCartridgeClient adgCartridgeClient = mock(AdgCartridgeClient.class);
    private final AdgCheckDataService adgCheckDataService = new AdgCheckDataService(adgCartridgeClient);

    @BeforeEach
    void setUp() {
        when(adgCartridgeClient.getCheckSumByInt32Hash(any(), any(), any(), any()))
                .thenReturn(Future.succeededFuture(RESULT));
    }

    @Test
    void testCheckByHash() {
        Entity entity = Entity.builder()
                .name("entity")
                .schema("schema")
                .fields(Collections.emptyList())
                .build();
        CheckDataByHashInt32Request request = CheckDataByHashInt32Request.builder()
                .sysCn(1L)
                .envName("env")
                .datamart("schema")
                .columns(Collections.singleton("column"))
                .entity(entity)
                .build();
        adgCheckDataService.checkDataByHashInt32(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adgCartridgeClient, times(1))
                            .getCheckSumByInt32Hash(
                                    eq(AdgUtils.getSpaceName(request.getEnvName(), entity.getSchema(), entity.getName(),
                                            ColumnFields.ACTUAL_POSTFIX)),
                                    eq(AdgUtils.getSpaceName(request.getEnvName(), entity.getSchema(), entity.getName(),
                                            ColumnFields.HISTORY_POSTFIX)),
                                    eq(request.getSysCn()), eq(request.getColumns()));
                });
    }

    @Test
    void testCheckByCount() {
        Entity entity = Entity.builder()
                .name("entity")
                .schema("schema")
                .fields(Collections.emptyList())
                .build();
        CheckDataByCountRequest request = CheckDataByCountRequest.builder()
                .sysCn(1L)
                .envName("env")
                .datamart("schema")
                .entity(entity)
                .build();
        adgCheckDataService.checkDataByCount(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adgCartridgeClient, times(1))
                            .getCheckSumByInt32Hash(
                                    eq(AdgUtils.getSpaceName(request.getEnvName(), entity.getSchema(), entity.getName(),
                                            ColumnFields.ACTUAL_POSTFIX)),
                                    eq(AdgUtils.getSpaceName(request.getEnvName(), entity.getSchema(), entity.getName(),
                                            ColumnFields.HISTORY_POSTFIX)),
                                    eq(request.getSysCn()), eq(null));
                });
    }
}
