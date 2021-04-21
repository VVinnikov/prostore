package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.SYS_FROM_ATTR;

public abstract class AbstractMppwRequestFactory implements MppwRequestFactory<AdbKafkaMppwTransferRequest> {

    @Override
    public AdbKafkaMppwTransferRequest create(MppwTransferDataRequest request) {
        String actualColumns = request.getColumnList().stream()
                .map(s -> "a." + s)
                .map(cn -> ("a.sys_to".equals(cn)) ? request.getHotDelta() - 1 + "" : cn)
                .map(cn -> ("a.sys_op".equals(cn)) ? "s.sys_op" : cn)
                .collect(Collectors.joining(","));
        String joinConditionInsert = request.getKeyColumnList().stream()
                .filter(columnName -> !SYS_FROM_ATTR.equals(columnName))
                .map(key -> "s." + key + "=" + "a." + key)
                .collect(Collectors.joining(" AND "));

        String joinConditionDelete = request.getKeyColumnList().stream()
                .filter(columnName -> !SYS_FROM_ATTR.equals(columnName))
                .map(key -> "a." + key + "=" + "s." + key)
                .collect(Collectors.joining(" AND "));

        String columnsString = String.join(",", request.getColumnList());

        String insertHistorySql = String.format(getInsertHistorySql(),
                request.getDatamart(), request.getTableName(), columnsString,
                actualColumns,
                request.getDatamart(), request.getTableName(),
                request.getDatamart(), request.getTableName(),
                joinConditionInsert);

        String deleteActualSql = String.format(getDeleteActualSql(),
                request.getDatamart(), request.getTableName(), request.getDatamart(), request.getTableName(),
                joinConditionDelete);

        String stagingColumnsString = String.join(",", getStagingColumnList(request));

        String insertActualSql = String.format(getInsertActualSql(),
                request.getDatamart(), request.getTableName(), columnsString,
                stagingColumnsString,
                request.getDatamart(), request.getTableName(),
                request.getDatamart(), request.getTableName());

        String truncateStagingSql = String.format(getTruncateStagingSql(),
                request.getDatamart(), request.getTableName());

        return new AdbKafkaMppwTransferRequest(
                Arrays.asList(
                        PreparedStatementRequest.onlySql(insertHistorySql),
                        PreparedStatementRequest.onlySql(deleteActualSql)),
                Arrays.asList(
                        PreparedStatementRequest.onlySql(insertActualSql),
                        PreparedStatementRequest.onlySql(truncateStagingSql)
                )
        );
    }

    protected abstract String getTruncateStagingSql();

    protected abstract String getInsertActualSql();

    protected abstract String getDeleteActualSql();

    protected abstract String getInsertHistorySql();

    private List<String> getStagingColumnList(MppwTransferDataRequest request) {
        return request.getColumnList().stream()
                .map(fieldName -> SYS_FROM_ATTR.equals(fieldName) ? String.valueOf(request.getHotDelta()) : fieldName)
                .collect(Collectors.toList());
    }
}
