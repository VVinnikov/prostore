package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.AdgConnectorApiProperties;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgLoadDataConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgLoadDataConnectorResponse;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgSubscriptionConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.connector.AdgTransferDataConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adg.exception.connector.AdgConnectorError;
import ru.ibs.dtm.query.execution.plugin.adg.exception.connector.AdgLoadDataConnectorError;
import ru.ibs.dtm.query.execution.plugin.adg.service.AdgConnectorApi;

@Slf4j
@Component
@RequiredArgsConstructor
public class AdgConnectorApiImpl implements AdgConnectorApi {
    private static final String STAGE_DATA_TABLE_NAME = "_stage_data_table_name";
    private static final String ACTUAL_DATA_TABLE_NAME = "_actual_data_table_name";
    private static final String HISTORICAL_DATA_TABLE_NAME = "_historical_data_table_name";
    private static final String DELTA_NUMBER = "_delta_number";
    private final AdgConnectorApiProperties connectorApiProperties;
    private final WebClient webClient;

    @Override
    public void subscribe(AdgSubscriptionConnectorRequest request, Handler<AsyncResult<Void>> handler) {
        val uri = connectorApiProperties.getAddress() + connectorApiProperties.getSubscriptionPath();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.postAbs(uri)
                .sendJson(request, ar -> {
                    if (ar.succeeded()) {
                        val response = ar.result();
                        handleSubscription(response, handler);
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void handleSubscription(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [subscription] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
                log.debug("Subscription Successful");
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgConnectorError.class)));
            } else {
                unexpectedError(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    @Override
    public void loadData(AdgLoadDataConnectorRequest request,
                         Handler<AsyncResult<AdgLoadDataConnectorResponse>> handler) {
        val uri = connectorApiProperties.getAddress() + connectorApiProperties.getLoadDataPath();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.postAbs(uri)
                .sendJson(request, ar -> {
                    if (ar.succeeded()) {
                        val response = ar.result();
                        handleLoadData(response, handler);
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void handleLoadData(HttpResponse<Buffer> response,
                                Handler<AsyncResult<AdgLoadDataConnectorResponse>> handler) {
        try {
            log.trace("handle [load data] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                val successResponse = response.bodyAsJson(AdgLoadDataConnectorResponse.class);
                handler.handle(Future.succeededFuture(successResponse));
                log.debug("Loading Successful");
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgLoadDataConnectorError.class)));
            } else if (statusCode == 404) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgConnectorError.class)));
            } else {
                unexpectedError(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    @Override
    public void transferDataToScdTable(AdgTransferDataConnectorRequest request, Handler<AsyncResult<Void>> handler) {
        val uri = connectorApiProperties.getAddress() + connectorApiProperties.getTransferDataToScdTablePath();
        log.debug("send to [{}] request [{}]", uri, request);
        val tableNames = request.getHelperTableNames();
        webClient.deleteAbs(uri)
                .addQueryParam(STAGE_DATA_TABLE_NAME, tableNames.getStaging())
                .addQueryParam(ACTUAL_DATA_TABLE_NAME, tableNames.getActual())
                .addQueryParam(HISTORICAL_DATA_TABLE_NAME, tableNames.getHistory())
                .addQueryParam(DELTA_NUMBER, String.valueOf(request.getDeltaNumber()))
                .send(ar -> {
                    if (ar.succeeded()) {
                        val response = ar.result();
                        log.trace("handle [transfer data to scd table] response [{}]", response);
                        val statusCode = response.statusCode();
                        if (statusCode == 200) {
                            handler.handle(Future.succeededFuture());
                        } else if (statusCode == 500) {
                            unexpectedError(handler, response);
                        }
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void cancelSubscription(String topicName, Handler<AsyncResult<Void>> handler) {
        val uri = connectorApiProperties.getAddress() + connectorApiProperties.getSubscriptionPath() + "/" + topicName;
        log.debug("send to [{}]", uri);
        webClient.deleteAbs(uri)
                .send(ar -> {
                    if (ar.succeeded()) {
                        val response = ar.result();
                        handleCancelSubscription(response, handler);
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void handleCancelSubscription(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [cancel subscription] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
            } else if (statusCode == 404 || statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgConnectorError.class)));
            } else {
                unexpectedError(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    private <T> void unexpectedError(Handler<AsyncResult<T>> handler, HttpResponse<Buffer> response) {
        String failureMessage = String.format("Unexpected response %s by request ()", response);
        handler.handle(Future.failedFuture(failureMessage));
    }

}
