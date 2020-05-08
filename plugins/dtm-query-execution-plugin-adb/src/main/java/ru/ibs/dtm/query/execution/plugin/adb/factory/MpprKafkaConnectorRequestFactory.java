package ru.ibs.dtm.query.execution.plugin.adb.factory;

import ru.ibs.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;

/**
 * Фабрика создания запросов к mpprConnector
 */
public interface MpprKafkaConnectorRequestFactory {

  MpprKafkaConnectorRequest create(MpprKafkaRequest mpprKafkaRequest, String enrichedQuery);
}
