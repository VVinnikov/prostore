package io.arenadata.dtm.query.execution.core.metrics.controller;

import io.arenadata.dtm.query.execution.core.metrics.service.MetricsManagementService;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

@Component
@Slf4j
public class MetricsController {

    private final MetricsManagementService metricsManagementService;

    @Autowired
    public MetricsController(MetricsManagementService metricsManagementService) {
        this.metricsManagementService = metricsManagementService;
    }

    public void turnOn(RoutingContext context) {
        try {
            String json = Json.encode(metricsManagementService.turnOnMetrics());
            context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(json);
        } catch (Exception e) {
            context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), e);
        }
    }

    public void turnOff(RoutingContext context) {
        try {
            String json = Json.encode(metricsManagementService.turnOffMetrics());
            context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(json);
        } catch (Exception e) {
            context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), e);
        }
    }
}
