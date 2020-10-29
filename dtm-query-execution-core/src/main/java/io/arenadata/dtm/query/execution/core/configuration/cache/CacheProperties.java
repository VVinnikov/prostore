package io.arenadata.dtm.query.execution.core.configuration.cache;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "core.cache")
public class CacheProperties {
    private Integer initialCapacity;
    private Integer maximumSize;
    private Integer expireAfterAccessMinutes;
}
