package io.arenadata.dtm.query.execution.core.configuration.hsql;

import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.service.hsql.impl.HSQLClientImpl;
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
