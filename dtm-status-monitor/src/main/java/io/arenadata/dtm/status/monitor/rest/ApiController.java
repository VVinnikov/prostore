package io.arenadata.dtm.status.monitor.rest;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.arenadata.dtm.status.monitor.kafka.KafkaMonitor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class ApiController {
    private final KafkaMonitor kafkaMonitor;

    public ApiController(KafkaMonitor kafkaMonitor) {
        this.kafkaMonitor = kafkaMonitor;
    }

    @PostMapping("/status")
    public StatusResponse status(@RequestBody StatusRequest request) {
        return kafkaMonitor.status(request);
    }
}
