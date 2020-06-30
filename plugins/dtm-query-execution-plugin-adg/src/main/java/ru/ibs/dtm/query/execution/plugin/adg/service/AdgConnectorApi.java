package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgLoadDataConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgLoadDataConnectorResponse;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgSubscriptionConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgTransferDataConnectorRequest;

public interface AdgConnectorApi {
    void subscribe(AdgSubscriptionConnectorRequest request, Handler<AsyncResult<Void>> handler);

    void loadData(AdgLoadDataConnectorRequest request,
                  Handler<AsyncResult<AdgLoadDataConnectorResponse>> handler);

    void transferDataToScdTable(AdgTransferDataConnectorRequest request,
                                Handler<AsyncResult<Void>> handler);

    void cancelSubscription(String topicName, Handler<AsyncResult<Void>> handler);
}
