package io.arenadata.dtm.status.monitor.rest;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.status.monitor.kafka.KafkaMonitor;
import io.arenadata.dtm.status.monitor.version.VersionService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/")
public class ApiController {
    private final KafkaMonitor kafkaMonitor;
    private final VersionService versionService;

    public ApiController(KafkaMonitor kafkaMonitor, VersionService versionService) {
        this.kafkaMonitor = kafkaMonitor;
        this.versionService = versionService;
    }

    @PostMapping("/status")
    public StatusResponse status(@RequestBody StatusRequest request) {
        return kafkaMonitor.status(request);
    }

    @GetMapping("/versions")
    public VersionInfo version() {
        return versionService.getVersionInfo();
    }
}
