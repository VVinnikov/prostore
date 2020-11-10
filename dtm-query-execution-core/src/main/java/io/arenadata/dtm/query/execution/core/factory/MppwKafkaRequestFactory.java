package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.vertx.core.Future;

public interface MppwKafkaRequestFactory {

    Future<MppwRequestContext> create(EdmlRequestContext context);
}
