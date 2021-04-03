package io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.dto.MpprKafkaConnectorRequest;
import io.vertx.core.Future;

/**
 * Service for connecting with mppr kafka connector component
 */
public interface MpprKafkaConnectorService {

    Future<QueryResult> call(MpprKafkaConnectorRequest request);
}
