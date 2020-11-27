package io.arenadata.dtm.query.execution.core.integration;

import io.arenadata.dtm.query.execution.core.integration.query.executor.QueryExecutor;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

@ExtendWith(VertxExtension.class)
public class EdmlIntegrationTest extends AbstractCoreDtmIntegrationTest {

    @Autowired
    @Qualifier("itTestQueryExecutor")
    private QueryExecutor queryExecutor;



}