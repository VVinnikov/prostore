package io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service.load;

import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.dto.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.dto.RestMppwKafkaStopRequest;
import io.vertx.core.Future;

public interface RestLoadClient {
    Future<Void> initiateLoading(RestMppwKafkaLoadRequest request);
    Future<Void> stopLoading(RestMppwKafkaStopRequest request);
}
