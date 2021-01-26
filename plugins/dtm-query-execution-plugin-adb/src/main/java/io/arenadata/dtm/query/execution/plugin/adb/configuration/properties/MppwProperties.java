package io.arenadata.dtm.query.execution.plugin.adb.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("adb.mppw")
@Component
public class MppwProperties {
    private String consumerGroup;
    private long stopTimeoutMs = 864_00_000L;
    private long fdwTimeoutMs = 1000L;
    private long defaultMessageLimit = 100L;
}
