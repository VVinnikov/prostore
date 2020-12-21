package io.arenadata.dtm.query.execution.core.kafka;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.vertx.core.Future;

public interface StatusMonitorService {

    Future<StatusResponse> getTopicStatus(String host, int port, StatusRequest statusRequest);
}
