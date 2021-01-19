package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;
import io.vertx.core.Future;

public interface MppwKafkaRequestFactory {

    Future<MppwPluginRequest> create(EdmlRequestContext context);
}
