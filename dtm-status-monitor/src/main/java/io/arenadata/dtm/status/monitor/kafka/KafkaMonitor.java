package io.arenadata.dtm.status.monitor.kafka;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;

import java.util.List;

public interface KafkaMonitor {
    StatusResponse status(StatusRequest request);
    List<StatusResponse> listAll();
}
