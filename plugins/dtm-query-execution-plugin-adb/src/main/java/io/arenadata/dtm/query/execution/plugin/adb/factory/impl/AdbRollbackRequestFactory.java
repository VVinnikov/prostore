package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Component
public class AdbRollbackRequestFactory implements RollbackRequestFactory<AdbRollbackRequest> {

    private static final String TRUNCATE_STAGING = "TRUNCATE %s.%s_staging";
    private static final String DELETE_FROM_ACTUAL = "DELETE FROM %s.%s_actual WHERE sys_from = %s";
    private static final String INSERT_ACTUAL_SQL = "INSERT INTO %s.%s_actual (%s, sys_from, sys_to, sys_op)\n" +
        "SELECT %s, sys_from, NULL, 0\n" +
        "FROM %s.%s_history\n" +
        "WHERE sys_to = %s";
    private static final String DELETE_FROM_HISTORY = "DELETE FROM %s.%s_history WHERE sys_to = %s";

    @Override
    public AdbRollbackRequest create(RollbackRequest rollbackRequest) {
        String truncateSql = String.format(TRUNCATE_STAGING,
            rollbackRequest.getDatamartMnemonic(), rollbackRequest.getDestinationTable());
        String deleteFromActualSql = String.format(DELETE_FROM_ACTUAL, rollbackRequest.getDatamartMnemonic(),
            rollbackRequest.getDestinationTable(), rollbackRequest.getSysCn());
        String fields = rollbackRequest.getEntity().getFields().stream()
            .map(EntityField::getName)
            .collect(Collectors.joining(","));
        long sysTo = rollbackRequest.getSysCn() - 1;
        String insertSql = String.format(INSERT_ACTUAL_SQL, rollbackRequest.getDatamartMnemonic(),
            rollbackRequest.getDestinationTable(), fields, fields,
            rollbackRequest.getDatamartMnemonic(), rollbackRequest.getDestinationTable(), sysTo);
        String deleteFromHistory = String.format(DELETE_FROM_HISTORY, rollbackRequest.getDatamartMnemonic(),
            rollbackRequest.getDestinationTable(), sysTo);

        return new AdbRollbackRequest(
            PreparedStatementRequest.onlySql(deleteFromHistory),
            PreparedStatementRequest.onlySql(deleteFromActualSql),
            PreparedStatementRequest.onlySql(truncateSql),
            PreparedStatementRequest.onlySql(insertSql)
        );
    }
}
