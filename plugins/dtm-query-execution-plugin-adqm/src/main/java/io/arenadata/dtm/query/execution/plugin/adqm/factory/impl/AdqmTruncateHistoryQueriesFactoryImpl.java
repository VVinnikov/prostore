package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmTruncateHistoryQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class AdqmTruncateHistoryQueriesFactoryImpl implements AdqmTruncateHistoryQueriesFactory {

    private static final String QUERY_PATTERN = "INSERT INTO %s.%s_actual (%s, sign)\n" +
            "SELECT %s, -1\n" +
            "FROM %s.%s_actual t FINAL\n" +
            "WHERE sign = 1%s%s";
    private static final String FLUSH_PATTERN = "SYSTEM FLUSH DISTRIBUTED %s.%s_actual";
    private static final String OPTIMIZE_PATTERN = "OPTIMIZE TABLE %s.%s_actual_shard ON CLUSTER %s FINAL";

    private final SqlDialect sqlDialect;
    private final DdlProperties ddlProperties;

    @Autowired
    public AdqmTruncateHistoryQueriesFactoryImpl(@Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                                                 DdlProperties ddlProperties) {
        this.sqlDialect = sqlDialect;
        this.ddlProperties = ddlProperties;
    }

    @Override
    public String insertIntoActualQuery(TruncateHistoryRequest params) {
        String sysCnExpression = params.getSysCn()
                .map(sysCn -> String.format(" AND sys_to < %s", sysCn))
                .orElse("");
        String whereExpression = params.getConditions()
                .map(conditions -> String.format(" AND (%s)", conditions.toSqlString(sqlDialect)))
                .orElse("");
        Entity entity = params.getEntity();
        String dbName = String.format("%s__%s", params.getEnvName(), entity.getSchema());
        List<String> orderByColumns = entity.getFields().stream()
                .filter(field -> field.getPrimaryOrder() != null)
                .map(EntityField::getName)
                .collect(Collectors.toList());
        orderByColumns.add(Constants.SYS_FROM_FIELD);
        String orderByColumnsStr = String.join(", ", orderByColumns);
        return String.format(QUERY_PATTERN, dbName, entity.getName(), orderByColumnsStr, orderByColumnsStr, dbName,
                        entity.getName(), sysCnExpression, whereExpression);
    }

    @Override
    public String flushQuery(TruncateHistoryRequest params) {
        Entity entity = params.getEntity();
        String dbName = String.format("%s__%s", params.getEnvName(), entity.getSchema());
        return String.format(FLUSH_PATTERN, dbName, entity.getName());
    }

    @Override
    public String optimizeQuery(TruncateHistoryRequest params) {
        Entity entity = params.getEntity();
        String dbName = String.format("%s__%s", params.getEnvName(), entity.getSchema());
        return String.format(OPTIMIZE_PATTERN, dbName, entity.getName(), ddlProperties.getCluster());
    }
}
