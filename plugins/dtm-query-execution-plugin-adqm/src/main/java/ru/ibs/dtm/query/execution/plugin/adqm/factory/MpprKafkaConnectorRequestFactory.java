package ru.ibs.dtm.query.execution.plugin.adqm.factory;

import ru.ibs.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;

/**
 * Фабрика создания запросов к mpprConnector
 */
public interface MpprKafkaConnectorRequestFactory {

    MpprKafkaConnectorRequest create(MpprRequest mpprRequest, String enrichedQuery);
}
