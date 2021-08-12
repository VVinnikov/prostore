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
package io.arenadata.dtm.query.execution.plugin.adp.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adp.check.service.AdpCheckDataService;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.AdpQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class AdpCheckDataServiceTest {
    private final static Long RESULT = 1L;
    private final AdpQueryExecutor adpQueryExecutor = mock(AdpQueryExecutor.class);
    private final AdpCheckDataService adpCheckDataService = new AdpCheckDataService(adpQueryExecutor);

    @BeforeEach
    void setUp() {
        when(adpQueryExecutor.executeUpdate(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testCheckByHash() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("hash_sum", RESULT);
        when(adpQueryExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));

        CheckDataByHashInt32Request request = CheckDataByHashInt32Request.builder()
                .sysCn(1L)
                .columns(Collections.emptySet())
                .entity(Entity.builder()
                        .fields(Collections.emptyList())
                        .build())
                .build();
        adpCheckDataService.checkDataByHashInt32(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adpQueryExecutor, times(1)).execute(any(), any());
                });
    }

    @Test
    void testCheckByCount() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("cnt", RESULT);
        when(adpQueryExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));

        CheckDataByCountRequest request = CheckDataByCountRequest.builder()
                .sysCn(1L)
                .entity(Entity.builder().build())
                .build();
        adpCheckDataService.checkDataByCount(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adpQueryExecutor, times(1)).execute(any(), any());
                });
    }
}