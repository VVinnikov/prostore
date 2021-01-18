package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprPluginRequest;
import io.vertx.core.Future;

public interface MpprKafkaRequestFactory {
    Future<MpprPluginRequest> create(EdmlRequestContext context);
}
