package io.arenadata.dtm.query.execution.plugin.adb.service.impl.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
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

class AdbCheckDataServiceTest {
    private final static Long RESULT = 1L;
    private final AdbQueryExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private final AdbCheckDataService adbCheckDataService = new AdbCheckDataService(adbQueryExecutor);

    @BeforeEach
    void setUp() {
        when(adbQueryExecutor.executeUpdate(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testCheckByHash() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("sum", RESULT);
        when(adbQueryExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));

        CheckDataByHashInt32Request request = CheckDataByHashInt32Request.builder()
                .sysCn(1L)
                .columns(Collections.emptySet())
                .entity(Entity.builder()
                        .fields(Collections.emptyList())
                        .build())
                .build();
        adbCheckDataService.checkDataByHashInt32(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adbQueryExecutor, times(1)).execute(any(), any());
                });
    }

    @Test
    void testCheckByCount() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("count(1)", RESULT);
        when(adbQueryExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));

        CheckDataByCountRequest request = CheckDataByCountRequest.builder()
                .sysCn(1L)
                .entity(Entity.builder().build())
                .build();
        adbCheckDataService.checkDataByCount(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adbQueryExecutor, times(1)).execute(any(), any());
                });
    }
}
