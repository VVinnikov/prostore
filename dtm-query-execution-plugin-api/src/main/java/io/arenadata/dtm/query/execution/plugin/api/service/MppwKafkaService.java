package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;
import io.vertx.core.Future;

public interface MppwKafkaService {
    Future<QueryResult> execute(MppwPluginRequest request);
}
