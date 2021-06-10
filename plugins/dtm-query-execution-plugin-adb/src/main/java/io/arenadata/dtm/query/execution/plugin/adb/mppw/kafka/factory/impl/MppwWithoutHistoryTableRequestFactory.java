package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTransferDataRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.*;

public class MppwWithoutHistoryTableRequestFactory extends AbstractMppwRequestFactory {

    private static final String STAGING_ALIAS = "staging.";
    private static final String INSERT_HISTORY_SQL = "UPDATE ${datamart}.${table}_actual actual\n" +
            "SET\n" +
            "  sys_to = ${sysFrom},\n" +
            "  sys_op = staging.sys_op\n" +
            "FROM (\n" +
            "  SELECT ${primaryKeyFields}, MAX(sys_op)\n" +
            "  FROM ${datamart}.${table}_staging\n" +
            "  GROUP BY ${primaryKeyFields}\n" +
            ") staging\n" +
            "WHERE ${primaryKeysCondition} AND\n" +
            "      actual.sys_from < ${sysCn} AND actual.sys_to IS NULL";
    private static final String INSERT_ACTUAL_SQL = "INSERT INTO ${datamart}.${table}_actual (${allFields})\n" +
            "  SELECT DISTINCT ON (${primaryKeyStageFields}) ${allStagingFields}, ${sysCn} AS sys_from, 0 AS sys_op FROM ${datamart}.${table}_staging staging\n" +
            "    LEFT JOIN ${datamart}.${table}_actual actual ON ${primaryKeysCondition} AND actual.sys_from = ${sysCn}\n" +
            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1";
    private static final String TRUNCATE_STAGING_SQL = "TRUNCATE %s.%s_staging";

    @Override
    public AdbKafkaMppwTransferRequest create(MppwTransferDataRequest request) {

        String sysFrom = String.valueOf(request.getHotDelta() - 1);
        String sysCn = String.valueOf(request.getHotDelta());
        String primaryKeyFields = request.getKeyColumnList().stream()
                .filter(columnName -> !SYS_FROM_ATTR.equals(columnName))
                .collect(Collectors.joining(", "));

        String primaryKeysCondition = request.getKeyColumnList().stream()
                .filter(columnName -> !SYS_FROM_ATTR.equals(columnName))
                .map(key -> STAGING_ALIAS + key + "=" + "actual." + key)
                .collect(Collectors.joining(" AND "));

        String insertHistorySql = INSERT_HISTORY_SQL
                .replace("${datamart}", request.getDatamart())
                .replace("${table}", request.getTableName())
                .replace("${sysFrom}", sysFrom)
                .replace("${sysCn}", sysCn)
                .replace("${primaryKeyFields}", primaryKeyFields)
                .replace("${primaryKeysCondition}", primaryKeysCondition);

        List<String> withoutSysFromAndOpColumns = request.getColumnList().stream()
                .filter(columnName -> !SYS_FROM_ATTR.equals(columnName)
                        && !SYS_OP_ATTR.equals(columnName)
                        && !SYS_TO_ATTR.equals(columnName))
                .collect(Collectors.toList());

        String allStagingFields = withoutSysFromAndOpColumns.stream()
                .map(cn -> STAGING_ALIAS + cn)
                .collect(Collectors.joining(", "));

        withoutSysFromAndOpColumns.add(SYS_FROM_ATTR);
        withoutSysFromAndOpColumns.add(SYS_OP_ATTR);

        String allColumnsStr = String.join(", ", withoutSysFromAndOpColumns);

        String primaryKeyStageFields = request.getKeyColumnList().stream()
                .filter(columnName -> !SYS_FROM_ATTR.equals(columnName))
                .map(cn -> STAGING_ALIAS + cn)
                .collect(Collectors.joining(", "));

        String insertActualSql = INSERT_ACTUAL_SQL
                .replace("${datamart}", request.getDatamart())
                .replace("${table}", request.getTableName())
                .replace("${allFields}", allColumnsStr)
                .replace("${primaryKeyStageFields}", primaryKeyStageFields)
                .replace("${allStagingFields}", allStagingFields)
                .replace("${sysCn}", sysCn)
                .replace("${primaryKeysCondition}", primaryKeysCondition);

        String truncateStagingSql = String.format(TRUNCATE_STAGING_SQL,
                request.getDatamart(), request.getTableName());

        return new AdbKafkaMppwTransferRequest(
                Collections.singletonList(PreparedStatementRequest.onlySql(insertHistorySql)),
                Arrays.asList(
                        PreparedStatementRequest.onlySql(insertActualSql),
                        PreparedStatementRequest.onlySql(truncateStagingSql)
                )
        );
    }
}
