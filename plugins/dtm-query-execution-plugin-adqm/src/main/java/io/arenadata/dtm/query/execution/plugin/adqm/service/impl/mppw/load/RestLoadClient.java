package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import io.vertx.core.Future;

public interface RestLoadClient {
    Future<Void> initiateLoading(RestMppwKafkaLoadRequest request);
    Future<Void> stopLoading(RestMppwKafkaStopRequest request);
}
