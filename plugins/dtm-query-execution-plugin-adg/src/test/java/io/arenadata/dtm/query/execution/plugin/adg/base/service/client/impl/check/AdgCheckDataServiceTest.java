package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adg.base.utils.ColumnFields;
import io.arenadata.dtm.query.execution.plugin.adg.check.service.AdgCheckDataService;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.base.utils.AdgUtils;
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
