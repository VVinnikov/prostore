package io.arenadata.dtm.query.execution.plugin.adqm.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;

/**
 * Фабрика создания запросов к mpprConnector
 */
public interface MpprKafkaConnectorRequestFactory {

    MpprKafkaConnectorRequest create(MpprRequest mpprRequest, String enrichedQuery);
}
