package io.arenadata.dtm.query.execution.plugin.adqm.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;

/**
 * Фабрика создания запросов к mpprConnector
 */
public interface MpprKafkaConnectorRequestFactory {

    MpprKafkaConnectorRequest create(MpprKafkaRequest mpprRequest, String enrichedQuery);
}
