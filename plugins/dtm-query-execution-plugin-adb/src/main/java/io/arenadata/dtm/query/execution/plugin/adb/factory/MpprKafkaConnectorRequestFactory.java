package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;

/**
 * Factory for making requests to  mpprConnector
 */
public interface MpprKafkaConnectorRequestFactory {

  MpprKafkaConnectorRequest create(MpprKafkaRequest mpprRequest, String enrichedQuery);
}
