package io.arenadata.dtm.query.execution.core.base.configuration;

import io.arenadata.dtm.query.execution.core.base.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.base.service.hsql.impl.HSQLClientImpl;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HSQLConfiguration {

    @Bean
    public HSQLClient hsqlClient(@Qualifier("coreVertx") Vertx vertx) {
        return new HSQLClientImpl(vertx);
    }
}
