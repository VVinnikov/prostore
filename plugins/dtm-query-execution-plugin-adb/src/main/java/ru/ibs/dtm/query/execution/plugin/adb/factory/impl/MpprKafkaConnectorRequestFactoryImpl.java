package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MpprKafkaConnectorRequestFactory;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;

@Component
public class MpprKafkaConnectorRequestFactoryImpl implements MpprKafkaConnectorRequestFactory {

  @Override
  public MpprKafkaConnectorRequest create(MpprRequest mpprRequest,
                                          String enrichedQuery) {
    QueryRequest queryRequest = mpprRequest.getQueryRequest();
    return new MpprKafkaConnectorRequest(
      queryRequest.getSql(),
      queryRequest.getDatamartMnemonic(),
      enrichedQuery,
      mpprRequest.getZookeeperHost(),
      String.valueOf(mpprRequest.getZookeeperPort()),
      mpprRequest.getTopic(),
      mpprRequest.getQueryExloadParam().getChunkSize());
  }
}
