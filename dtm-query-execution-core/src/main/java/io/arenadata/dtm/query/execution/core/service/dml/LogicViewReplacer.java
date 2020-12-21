package io.arenadata.dtm.query.execution.core.service.dml;

import io.vertx.core.Future;

public interface LogicViewReplacer {
    Future<String> replace(String sql, String datamart);
}
