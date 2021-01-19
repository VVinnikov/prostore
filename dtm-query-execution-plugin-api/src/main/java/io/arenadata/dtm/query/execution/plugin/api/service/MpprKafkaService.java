package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprPluginRequest;
import io.vertx.core.Future;


public interface MpprKafkaService {
    Future<QueryResult> execute(MpprPluginRequest request);
}
