package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgTruncateHistoryConditionFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class AdgTruncateHistoryConditionFactoryImpl implements AdgTruncateHistoryConditionFactory {

    private static final String SYS_CN_CONDITION_PATTERN = "\"sys_to\" < %s";

    private final SqlDialect sqlDialect;

    @Autowired
    public AdgTruncateHistoryConditionFactoryImpl(@Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<String> create(TruncateHistoryParams params) {
        List<String> conditions = new ArrayList<>();
        params.getConditions()
                .map(val -> String.format("(%s)", val.toSqlString(sqlDialect)))
                .ifPresent(conditions::add);
        params.getSysCn()
                .map(sysCn -> String.format(SYS_CN_CONDITION_PATTERN, sysCn))
                .ifPresent(conditions::add);
        return Future.succeededFuture(String.join(" AND ", conditions));
    }
}