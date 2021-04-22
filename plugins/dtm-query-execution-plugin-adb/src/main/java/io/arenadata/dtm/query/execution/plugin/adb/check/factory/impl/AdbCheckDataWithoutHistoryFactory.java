package io.arenadata.dtm.query.execution.plugin.adb.check.factory.impl;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataByHashFieldValueFactory;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataQueryFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import lombok.val;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AdbCheckDataWithoutHistoryFactory implements AdbCheckDataQueryFactory {

    private final AdbCheckDataByHashFieldValueFactory checkDataByHashFieldValueFactory;

    private static final String CHECK_DATA_BY_COUNT_TEMPLATE = "SELECT count(1) as %s FROM " +
            "(SELECT 1 " +
            "FROM %s.%s_actual " +
            "WHERE (sys_to = %d AND sys_op = 1) OR sys_from = %d) AS tmp";

    private static final String CHECK_DATA_BY_HASH_TEMPLATE =
            "SELECT sum(dtmInt32Hash(MD5(concat(%s))::bytea)) as %s FROM\n" +
                    "(\n" +
                    "  SELECT %s \n" +
                    "  FROM %s.%s_history \n" +
                    "  WHERE (sys_to = %d AND sys_op = 1) OR sys_from = %d) AS tmp";

    public AdbCheckDataWithoutHistoryFactory(AdbCheckDataByHashFieldValueFactory checkDataByHashFieldValueFactory) {
        this.checkDataByHashFieldValueFactory = checkDataByHashFieldValueFactory;
    }

    @Override
    public String createCheckDataByCountQuery(CheckDataByCountRequest request, String resultColumnName) {
        return String.format(CHECK_DATA_BY_COUNT_TEMPLATE,
                resultColumnName,
                request.getEntity().getSchema(),
                request.getEntity().getName(),
                request.getSysCn() - 1,
                request.getSysCn());
    }

    @Override
    public String createCheckDataByHashInt32Query(CheckDataByHashInt32Request request, String resultColumnName) {
        Map<String, EntityField> fields = request.getEntity().getFields().stream()
                .collect(Collectors.toMap(EntityField::getName, Function.identity()));
        val fieldsConcatenationList = request.getColumns().stream()
                .map(fields::get)
                .map(checkDataByHashFieldValueFactory::create)
                .collect(Collectors.joining(",';',"));
        val columnsList = String.join(",';',", request.getColumns());
        val datamart = request.getEntity().getSchema();
        val table = request.getEntity().getName();
        val sysCn = request.getSysCn();
        return String.format(CHECK_DATA_BY_HASH_TEMPLATE,
                resultColumnName,
                fieldsConcatenationList,
                columnsList,
                datamart,
                table,
                sysCn - 1,
                sysCn);
    }
}
