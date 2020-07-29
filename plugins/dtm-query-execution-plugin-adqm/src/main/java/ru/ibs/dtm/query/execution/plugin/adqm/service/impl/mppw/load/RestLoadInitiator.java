package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import io.vertx.core.Future;

public interface RestLoadInitiator {
    Future<Void> initiateLoading(RestLoadRequest request);
}
