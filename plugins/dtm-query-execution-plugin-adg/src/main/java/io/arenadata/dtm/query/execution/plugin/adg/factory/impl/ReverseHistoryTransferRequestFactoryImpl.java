package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.AdgRollbackProperties;
import io.arenadata.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.factory.ReverseHistoryTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ReverseHistoryTransferRequestFactoryImpl implements ReverseHistoryTransferRequestFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;
    private final AdgRollbackProperties rollbackProperties;

    @Override
    public ReverseHistoryTransferRequest create(RollbackRequest request) {
        val envName = request.getEnvName();
        val tableName = request.getDestinationTable();
        val datamart = request.getDatamartMnemonic();
        val helperTableNames = helperTableNamesFactory.create(envName, datamart, tableName);
        return ReverseHistoryTransferRequest.builder()
            .eraseOperationBatchSize(rollbackProperties.getEraseOperationBatchSize())
            .stagingTableName(helperTableNames.getStaging())
            .historyTableName(helperTableNames.getHistory())
            .actualTableName(helperTableNames.getActual())
            .sysCn(request.getSysCn())
            .build();
    }
}
