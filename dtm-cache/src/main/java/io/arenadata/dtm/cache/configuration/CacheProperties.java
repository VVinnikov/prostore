package io.arenadata.dtm.cache.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component("cacheProperties")
@ConfigurationProperties(prefix = "core.cache")
@Data
public class CacheProperties {
    private Integer initialCapacity;
    private Integer maximumSize;
    private Integer expireAfterAccessMinutes;
}


