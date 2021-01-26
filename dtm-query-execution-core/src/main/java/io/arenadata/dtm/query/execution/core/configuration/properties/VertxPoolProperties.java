package io.arenadata.dtm.query.execution.core.configuration.properties;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("core.vertx.pool")
@Data
public class VertxPoolProperties {
    private Long taskTimeout = 864_00_000L;
}
