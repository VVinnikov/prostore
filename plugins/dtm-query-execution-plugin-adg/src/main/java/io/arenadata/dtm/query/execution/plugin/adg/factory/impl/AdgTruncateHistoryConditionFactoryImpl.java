package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgTruncateHistoryConditionFactory;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class AdgTruncateHistoryConditionFactoryImpl implements AdgTruncateHistoryConditionFactory {

    private static final String SYS_CN_CONDITION_PATTERN = "\"sys_to\" < %s";

    private final SqlDialect sqlDialect;

    @Autowired
    public AdgTruncateHistoryConditionFactoryImpl(@Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<String> create(Optional<SqlNode> reqConditions, Optional<Long> reqSysCn) {
        List<String> conditions = new ArrayList<>();
        reqConditions
                .map(val -> String.format("(%s)", val.toSqlString(sqlDialect)))
                .ifPresent(conditions::add);
        reqSysCn
                .map(sysCn -> String.format(SYS_CN_CONDITION_PATTERN, sysCn))
                .ifPresent(conditions::add);
        return Future.succeededFuture(String.join(" AND ", conditions));
    }
}
