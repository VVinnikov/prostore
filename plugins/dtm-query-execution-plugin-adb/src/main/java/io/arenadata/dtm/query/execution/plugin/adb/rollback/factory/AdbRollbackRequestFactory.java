package io.arenadata.dtm.query.execution.plugin.adb.rollback.factory;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;

import java.util.stream.Collectors;

public abstract class AdbRollbackRequestFactory implements RollbackRequestFactory<AdbRollbackRequest> {

    @Override
    public AdbRollbackRequest create(RollbackRequest rollbackRequest) {
        String truncateSql = String.format(getTruncateStagingSql(),
            rollbackRequest.getDatamartMnemonic(), rollbackRequest.getDestinationTable());
        String deleteFromActualSql = String.format(getDeleteFromActualSql(), rollbackRequest.getDatamartMnemonic(),
            rollbackRequest.getDestinationTable(), rollbackRequest.getSysCn());
        String fields = rollbackRequest.getEntity().getFields().stream()
            .map(EntityField::getName)
            .collect(Collectors.joining(","));
        long sysTo = rollbackRequest.getSysCn() - 1;
        String insertSql = String.format(getInsertActualSql(), rollbackRequest.getDatamartMnemonic(),
            rollbackRequest.getDestinationTable(), fields, fields,
            rollbackRequest.getDatamartMnemonic(), rollbackRequest.getDestinationTable(), sysTo);
        String deleteFromHistory = String.format(getDeleteFromHistorySql(), rollbackRequest.getDatamartMnemonic(),
            rollbackRequest.getDestinationTable(), sysTo);

        return new AdbRollbackRequest(
            PreparedStatementRequest.onlySql(deleteFromHistory),
            PreparedStatementRequest.onlySql(deleteFromActualSql),
            PreparedStatementRequest.onlySql(truncateSql),
            PreparedStatementRequest.onlySql(insertSql)
        );
    }

    protected abstract String getTruncateStagingSql();

    protected abstract String getDeleteFromActualSql();

    protected abstract String getInsertActualSql();

    protected abstract String getDeleteFromHistorySql();
}
