package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.vertx.core.Future;

public interface ViewReplacer {
    // TODO remove
    Future<String> replace(String sql, String datamart);

    Future<Void> replace(ViewReplaceContext context);

}
