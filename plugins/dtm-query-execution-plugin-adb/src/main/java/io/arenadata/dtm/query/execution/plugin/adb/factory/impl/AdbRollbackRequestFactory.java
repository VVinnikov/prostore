package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;
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
            rollbackRequest.getDatamart(), rollbackRequest.getTargetTable());
        String deleteFromActualSql = String.format(DELETE_FROM_ACTUAL, rollbackRequest.getDatamart(),
            rollbackRequest.getTargetTable(), rollbackRequest.getSysCn());
        String fields = rollbackRequest.getEntity().getFields().stream()
            .map(EntityField::getName)
            .collect(Collectors.joining(","));
        long sysTo = rollbackRequest.getSysCn() - 1;
        String insertSql = String.format(INSERT_ACTUAL_SQL, rollbackRequest.getDatamart(),
            rollbackRequest.getTargetTable(), fields, fields,
            rollbackRequest.getDatamart(), rollbackRequest.getTargetTable(), sysTo);
        String deleteFromHistory = String.format(DELETE_FROM_HISTORY, rollbackRequest.getDatamart(),
            rollbackRequest.getTargetTable(), sysTo);

        return new AdbRollbackRequest(
            PreparedStatementRequest.onlySql(deleteFromActualSql),
            PreparedStatementRequest.onlySql(deleteFromHistory),
            PreparedStatementRequest.onlySql(truncateSql),
            PreparedStatementRequest.onlySql(insertSql)
        );
    }
}
