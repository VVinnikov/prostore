package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.configuration.AdgRollbackProperties;
import io.arenadata.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.factory.ReverseHistoryTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ReverseHistoryTransferRequestFactoryImpl implements ReverseHistoryTransferRequestFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;
    private final AdgRollbackProperties rollbackProperties;

    @Override
    public ReverseHistoryTransferRequest create(RollbackRequestContext context) {
        val envName = context.getRequest().getQueryRequest().getEnvName();
        val tableName = context.getRequest().getTargetTable();
        val datamart = context.getRequest().getDatamart();
        val helperTableNames = helperTableNamesFactory.create(envName, datamart, tableName);
        return ReverseHistoryTransferRequest.builder()
            .eraseOperationBatchSize(rollbackProperties.getEraseOperationBatchSize())
            .stagingTableName(helperTableNames.getStaging())
            .historyTableName(helperTableNames.getHistory())
            .actualTableName(helperTableNames.getActual())
            .sysCn(context.getRequest().getSysCn())
            .build();
    }
}
