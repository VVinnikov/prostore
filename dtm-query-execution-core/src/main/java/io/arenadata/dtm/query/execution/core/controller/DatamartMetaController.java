package io.arenadata.dtm.query.execution.core.controller;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.execution.core.dto.request.RequestParam;
import io.arenadata.dtm.query.execution.core.service.metadata.DatamartMetaService;
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

@Component
@Slf4j
public class DatamartMetaController {

    private final DatamartMetaService datamartMetaService;

    @Autowired
    public DatamartMetaController(DatamartMetaService datamartMetaService) {
        this.datamartMetaService = datamartMetaService;
    }

    public void getDatamartMeta(RoutingContext context) {
        datamartMetaService.getDatamartMeta(
        );
    }

    public void getDatamartEntityMeta(RoutingContext context) {
        datamartMetaService.getEntitiesMeta(getDatamartMnemonic(context)
        );
    }

    public void getEntityAttributesMeta(RoutingContext context) {
        datamartMetaService.getAttributesMeta(getDatamartMnemonic(context),
                getParam(context, RequestParam.ENTITY_MNEMONIC)
        );
    }

    private String getDatamartMnemonic(RoutingContext context) {
        return getParam(context, RequestParam.DATAMART_MNEMONIC);
    }

    private String getParam(RoutingContext context, String paramName) {
        return context.request().getParam(paramName);
    }

    private static class ListToJsonHandler<T> implements AsyncHandler<List<T>> {
        private final RoutingContext context;
        private final String successLogMessage;

        ListToJsonHandler(RoutingContext context, String successLogMessage) {
            this.context = context;
            this.successLogMessage = successLogMessage;
        }

        @Override
        public void handle(AsyncResult<List<T>> asyncResult) {
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
}
