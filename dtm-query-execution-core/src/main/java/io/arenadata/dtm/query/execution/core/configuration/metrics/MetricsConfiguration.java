package io.arenadata.dtm.query.execution.core.configuration.metrics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.reactive.server.ReactiveWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfiguration {

    private final Integer metricsServerPort;

    @Autowired
    public MetricsConfiguration(@Value("${management.server.port}") String metricsServerPort) {
        this.metricsServerPort = Integer.valueOf(metricsServerPort);
    }

    @Bean
    public ReactiveWebServerFactory reactiveWebServerFactory() {
        return new NettyReactiveWebServerFactory(this.metricsServerPort);
    }


}
