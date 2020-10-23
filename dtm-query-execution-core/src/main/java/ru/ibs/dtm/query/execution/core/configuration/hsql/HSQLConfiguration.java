package ru.ibs.dtm.query.execution.core.configuration.hsql;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.core.service.hsql.HSQLClient;
import ru.ibs.dtm.query.execution.core.service.hsql.impl.HSQLClientImpl;

@Configuration
public class HSQLConfiguration {

    @Bean
    public HSQLClient hsqlClient(@Qualifier("coreVertx") Vertx vertx) {
        return new HSQLClientImpl(vertx);
    }
}
