package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.AdgRollbackProperties;
import io.arenadata.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class ReverseHistoryTransferRequestFactoryImplTest {
    private static final ReverseHistoryTransferRequest EXPECTED_RQ = ReverseHistoryTransferRequest.builder()
        .historyTableName("env1__dtm1__tbl1_history")
        .stagingTableName("env1__dtm1__tbl1_staging")
        .actualTableName("env1__dtm1__tbl1_actual")
        .eraseOperationBatchSize(300)
        .sysCn(11)
        .build();

    @Test
    void create() {
        val factory = new ReverseHistoryTransferRequestFactoryImpl(
            new AdgHelperTableNamesFactoryImpl(),
            new AdgRollbackProperties()
        );
        val request = factory.create(new RollbackRequestContext(RollbackRequest.builder()
            .sysCn(11)
            .destinationTable("tbl1")
            .datamart("dtm1")
            .queryRequest(QueryRequest.builder()
                .envName("env1")
                .build())
            .build()));
        log.info(request.toString());
        assertEquals(EXPECTED_RQ, request);
    }
}
