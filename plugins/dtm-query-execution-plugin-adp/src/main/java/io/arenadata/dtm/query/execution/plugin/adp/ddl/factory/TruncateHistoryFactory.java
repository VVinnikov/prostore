package io.arenadata.dtm.query.execution.plugin.adp.ddl.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.plugin.adp.base.Constants.ACTUAL_TABLE;

@Component
public class TruncateHistoryFactory {

    private static final String DELETE_RECORDS_PATTERN = "DELETE FROM %s.%s_%s%s";
    private static final String SYS_CN_CONDITION = "sys_to < %s";
    private final SqlDialect sqlDialect;

    public TruncateHistoryFactory(@Qualifier("adpSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    public String create(TruncateHistoryRequest request) {
        String whereExpression = request.getConditions()
                .map(conditions -> String.format(" WHERE %s", conditions.toSqlString(sqlDialect)))
                .orElse("");
        Entity entity = request.getEntity();
        return String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                ACTUAL_TABLE, whereExpression);
    }

    public String createWithSysCn(TruncateHistoryRequest request) {
        Entity entity = request.getEntity();
        return String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                ACTUAL_TABLE, String.format(" WHERE %s%s", request.getConditions()
                                .map(conditions -> String.format("%s AND ", conditions.toSqlString(sqlDialect)))
                                .orElse(""),
                        String.format(SYS_CN_CONDITION, request.getSysCn().get())));
    }

}