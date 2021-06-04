package io.arenadata.dtm.query.execution.plugin.adb.rollback.factory;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AdbRollbackRequestFactory implements RollbackRequestFactory<AdbRollbackRequest> {

    @Override
    public AdbRollbackRequest create(RollbackRequest rollbackRequest) {
        String truncateSql = String.format(getTruncateStagingSql(),
            rollbackRequest.getDatamartMnemonic(), rollbackRequest.getDestinationTable());
        String deleteFromActualSql = String.format(getDeleteFromActualSql(), rollbackRequest.getDatamartMnemonic(),
            rollbackRequest.getDestinationTable(), rollbackRequest.getSysCn());
        List<PreparedStatementRequest> eraseOps = getEraseSql(rollbackRequest)
                .stream()
                .map(sql -> PreparedStatementRequest.onlySql(sql))
                .collect(Collectors.toList());

        return new AdbRollbackRequest(
            PreparedStatementRequest.onlySql(truncateSql),
            PreparedStatementRequest.onlySql(deleteFromActualSql),
            eraseOps
        );
    }

    protected abstract String getTruncateStagingSql();

    protected abstract String getDeleteFromActualSql();

    protected abstract List<String> getEraseSql(RollbackRequest rollbackRequest);

}
