package io.arenadata.dtm.query.execution.core.query.controller;

import io.arenadata.dtm.query.execution.core.base.dto.request.RequestParam;
import io.arenadata.dtm.query.execution.core.base.service.metadata.DatamartMetaService;
import io.arenadata.dtm.query.execution.core.query.utils.LoggerContextUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

import java.util.List;
import java.util.UUID;

@Component
@Slf4j
public class DatamartMetaController {

    private final DatamartMetaService datamartMetaService;

    @Autowired
    public DatamartMetaController(DatamartMetaService datamartMetaService) {
        this.datamartMetaService = datamartMetaService;
    }

    public void getDatamartMeta(RoutingContext context) {
        LoggerContextUtils.setRequestId(UUID.randomUUID());
        datamartMetaService.getDatamartMeta()
                .onComplete(result -> handleData(context, "Reply sent with datamarts {}", result));
    }

    public void getDatamartEntityMeta(RoutingContext context) {
        LoggerContextUtils.setRequestId(UUID.randomUUID());
        datamartMetaService.getEntitiesMeta(getDatamartMnemonic(context))
                .onComplete(result -> handleData(context, "Reply sent with entities {}", result));
    }

    public void getEntityAttributesMeta(RoutingContext context) {
        LoggerContextUtils.setRequestId(UUID.randomUUID());
        datamartMetaService.getAttributesMeta(getDatamartMnemonic(context),
                getParam(context, RequestParam.ENTITY_MNEMONIC))
                .onComplete(result -> handleData(context, "Reply sent with attributes {}", result));
    }

    private String getDatamartMnemonic(RoutingContext context) {
        return getParam(context, RequestParam.DATAMART_MNEMONIC);
    }

    private String getParam(RoutingContext context, String paramName) {
        return context.request().getParam(paramName);
    }

    private <T> void handleData(RoutingContext context, String successLogMessage, AsyncResult<List<T>> asyncResult) {
        if (asyncResult.succeeded()) {
            String json = Json.encode(asyncResult.result());
            log.info(successLogMessage, json);
            context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(json);
        } else {
            context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), asyncResult.cause());
        }
    }

}
