package ru.ibs.dtm.status.monitor.kafka;

import ru.ibs.dtm.common.status.kafka.StatusRequest;
import ru.ibs.dtm.common.status.kafka.StatusResponse;

import java.util.List;

public interface KafkaMonitor {
    StatusResponse status(StatusRequest request);
    List<StatusResponse> listAll();
}
