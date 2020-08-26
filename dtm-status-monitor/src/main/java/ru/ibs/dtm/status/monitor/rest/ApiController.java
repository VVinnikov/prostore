package ru.ibs.dtm.status.monitor.rest;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.ibs.dtm.status.monitor.dto.StatusRequest;
import ru.ibs.dtm.status.monitor.dto.StatusResponse;
import ru.ibs.dtm.status.monitor.kafka.KafkaMonitor;

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
