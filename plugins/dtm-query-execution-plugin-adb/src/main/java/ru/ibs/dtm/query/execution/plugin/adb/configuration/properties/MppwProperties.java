package ru.ibs.dtm.query.execution.plugin.adb.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("adb.mppw")
@Component
public class MppwProperties {
    private String consumerGroup;
    private String startLoadUrl;
    private Integer poolSize;
    private Long stopTimeoutMs = 864_00_000L;
}
