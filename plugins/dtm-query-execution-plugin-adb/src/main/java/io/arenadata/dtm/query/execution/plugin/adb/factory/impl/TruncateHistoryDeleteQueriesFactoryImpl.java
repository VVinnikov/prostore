package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.factory.TruncateHistoryDeleteQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
public class TruncateHistoryDeleteQueriesFactoryImpl implements TruncateHistoryDeleteQueriesFactory {

    private static final String DELETE_RECORDS_PATTERN = "DELETE FROM %s.%s_%s%s";
    private static final String SYS_CN_CONDITION = "sys_to < %s";
    private final SqlDialect sqlDialect;

    @Autowired
    public TruncateHistoryDeleteQueriesFactoryImpl(@Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public List<String> create(TruncateHistoryRequest params) {
        String whereExpression = params.getConditions()
                .map(conditions -> String.format(" WHERE %s", conditions.toSqlString(sqlDialect)))
                .orElse("");
        Entity entity = params.getEntity();
        return Arrays.asList(String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                AdbTables.ACTUAL_TABLE_POSTFIX, whereExpression),
                String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                        AdbTables.HISTORY_TABLE_POSTFIX, whereExpression));
    }

    @Override
    public String createWithSysCn(TruncateHistoryRequest params) {
        Entity entity = params.getEntity();
        return String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                AdbTables.HISTORY_TABLE_POSTFIX, String.format(" WHERE %s%s", params.getConditions()
                                .map(conditions -> String.format("%s AND ", conditions.toSqlString(sqlDialect)))
                                .orElse(""),
                        String.format(SYS_CN_CONDITION, params.getSysCn().get())));
    }
}
