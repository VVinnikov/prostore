package io.arenadata.dtm.query.execution.core.verticle.configuration.properties;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("core.vertx.pool")
@Data
public class VertxPoolProperties {
    private Integer taskPool = 10;
    private Long taskTimeout = 864_00_000L;
}
