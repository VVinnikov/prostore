package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import io.reactiverse.pgclient.Tuple;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.PreparedStatementRequest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MppwRequestFactoryImpl implements MppwRequestFactory {

    String INSERT_HISTORY_SQL = "INSERT INTO %s.%s_history (%s)\n" +
            "SELECT %s\n" +
            "FROM %s.%s_actual a\n" +
            "         INNER JOIN %s.%s_staging s ON\n" +
            "    %s";

    String DELETE_ACTUAL_SQL = "DELETE\n" +
            "FROM %s.%s_actual a USING %s.%s_staging s\n" +
            "WHERE %s";

    String INSERT_ACTUAL_SQL = "INSERT INTO %s.%s_actual (%s)\n" +
            "SELECT %s\n" +
            "FROM %s.%s_staging\n" +
            "WHERE %s.%s_staging.sys_op <> 1";

    String TRUNCATE_STAGING_SQL = "TRUNCATE %s.%s_staging";

    @Override
    public List<PreparedStatementRequest> create(MppwTransferDataRequest request) {
        String actualColumns = request.getColumnList().stream()
                .map(s -> "a." + s)
                .map(cn -> ("a.sys_to".equals(cn)) ? request.getHotDelta() - 1 + "" : cn)
                .map(cn -> ("a.sys_op".equals(cn)) ? "s.sys_op" : cn)
                .collect(Collectors.joining(","));
        String joinConditionInsert = request.getKeyColumnList().stream()
                .map(key -> {
                    return "s." + key + "=" + "a." + key;
                })
                .collect(Collectors.joining(" AND "));
        String joinConditionDelete = request.getKeyColumnList().stream()
                .map(key -> "a." + key + "=" + "s." + key)
                .collect(Collectors.joining(" AND "));
        String columnsString = String.join(",", request.getColumnList());

        String insertHistorySql = String.format(INSERT_HISTORY_SQL,
                request.getDatamart(), request.getTableName(), columnsString,
                actualColumns,
                request.getDatamart(), request.getTableName(),
                request.getDatamart(), request.getTableName(),
                joinConditionInsert);

        String deleteActualSql = String.format(DELETE_ACTUAL_SQL,
                request.getDatamart(), request.getTableName(), request.getDatamart(), request.getTableName(),
                joinConditionDelete);

        String insertActualSql = String.format(INSERT_ACTUAL_SQL,
                request.getDatamart(), request.getTableName(), columnsString,
                columnsString,
                request.getDatamart(), request.getTableName(),
                request.getDatamart(), request.getTableName());

        String truncateStagingSql = String.format(TRUNCATE_STAGING_SQL,
                request.getDatamart(), request.getTableName());

        return Arrays.asList(
                new PreparedStatementRequest(insertHistorySql, Tuple.tuple()),
                new PreparedStatementRequest(deleteActualSql, Tuple.tuple()),
                new PreparedStatementRequest(insertActualSql, Tuple.tuple()),
                new PreparedStatementRequest(truncateStagingSql, Tuple.tuple())
        );
    }
}
