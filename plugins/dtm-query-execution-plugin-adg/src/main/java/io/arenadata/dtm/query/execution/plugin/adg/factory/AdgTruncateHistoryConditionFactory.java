package io.arenadata.dtm.query.execution.plugin.adg.factory;

import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

import java.util.Optional;

public interface AdgTruncateHistoryConditionFactory {

    Future<String> create(Optional<SqlNode> reqConditions, Optional<Long> reqSysCn);
}
