package io.arenadata.dtm.query.execution.plugin.adg.rollback.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adg.rollback.configuration.properties.AdgRollbackProperties;
import io.arenadata.dtm.query.execution.plugin.adg.rollback.dto.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
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
        val request = factory.create(
                RollbackRequest.builder()
                        .sysCn(11)
                        .destinationTable("tbl1")
                        .datamartMnemonic("dtm1")
                        .envName("env1")
                        .build());
        log.info(request.toString());
        assertEquals(EXPECTED_RQ, request);
    }
}