package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.vertx.core.Future;

public interface ViewReplacer {

    Future<Void> replace(ViewReplaceContext context);

}
