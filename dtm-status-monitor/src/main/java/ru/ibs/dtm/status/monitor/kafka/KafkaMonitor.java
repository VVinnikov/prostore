package ru.ibs.dtm.status.monitor.kafka;

import ru.ibs.dtm.status.monitor.dto.StatusRequest;
import ru.ibs.dtm.status.monitor.dto.StatusResponse;

import java.util.List;

public interface KafkaMonitor {
    StatusResponse status(StatusRequest request);
    List<StatusResponse> listAll();
}
