package ru.ibs.dtm.query.execution.plugin.adqm.service;

import io.vertx.core.json.JsonObject;

public interface StatusReporter {
    void onStart(JsonObject payload);
    void onFinish(JsonObject payload);
    void onError(JsonObject payload);
}
