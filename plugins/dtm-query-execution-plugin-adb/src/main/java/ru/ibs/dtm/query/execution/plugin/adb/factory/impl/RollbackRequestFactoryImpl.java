package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import io.reactiverse.pgclient.Tuple;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.query.execution.plugin.adb.factory.RollbackRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.RollbackRequest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class RollbackRequestFactoryImpl implements RollbackRequestFactory {

    private static final String TRUNCATE_STAGING = "TRUNCATE %s.%s_staging";
    private static final String DELETE_FROM_ACTUAL = "DELETE FROM %s.%s_actual WHERE sys_from = %s";
    private static final String INSERT_ACTUAL_SQL = "INSERT INTO %s.%s_actual (%s, sys_from, sys_to, sys_op)\n" +
            "SELECT %s, sys_from, NULL, 0\n" +
            "FROM %s.%s_history\n" +
            "WHERE sys_to = %s";
    private static final String DELETE_FROM_HISTORY = "DELETE FROM %s.%s_history WHERE sys_to = %s";

    @Override
    public List<PreparedStatementRequest> create(RollbackRequest rollbackRequest) {
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
        return Arrays.asList(
                new PreparedStatementRequest(truncateSql, Tuple.tuple()),
                new PreparedStatementRequest(deleteFromActualSql, Tuple.tuple()),
                new PreparedStatementRequest(insertSql, Tuple.tuple()),
                new PreparedStatementRequest(deleteFromHistory, Tuple.tuple())
        );
    }
}
