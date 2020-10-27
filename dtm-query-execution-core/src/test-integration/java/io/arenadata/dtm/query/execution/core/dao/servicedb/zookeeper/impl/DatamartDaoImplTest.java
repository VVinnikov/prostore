package io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl;

import io.arenadata.dtm.query.execution.core.configuration.properties.ZookeeperProperties;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatamartDaoImplTest {
    public static final String EXPECTED_DTM = "dtm1";
    private final DatamartDaoImpl dao;

    public DatamartDaoImplTest() {
        val connectionManager = new ZookeeperConnectionProviderImpl(getZookeeperProperties(), "TEST");
        val executor = new ZookeeperExecutorImpl(connectionManager, Vertx.vertx());
        dao = new DatamartDaoImpl(executor, "test1");
    }

    private ZookeeperProperties getZookeeperProperties() {
        ZookeeperProperties properties = new ZookeeperProperties();
        properties.setSessionTimeoutMs(864_000);
        properties.setConnectionString("localhost");
        properties.setConnectionTimeoutMs(10_000);
        properties.setChroot("/testgration");
        return properties;
    }

    @Test
    void createDatamart() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.deleteDatamart(EXPECTED_DTM)
            .otherwise((Void) null)
            .compose(v -> dao.createDatamart(EXPECTED_DTM))
            .compose(v -> dao.getDatamart(EXPECTED_DTM))
            .compose(v -> dao.getDatamarts())
            .compose(names -> {
                assertTrue(names.contains(EXPECTED_DTM));
                return dao.deleteDatamart(EXPECTED_DTM);
            })
            .onSuccess(s -> testContext.completeNow())
            .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertFalse(testContext.failed());
    }

    @Test
    void datamartAlreadyExists() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.deleteDatamart(EXPECTED_DTM)
            .otherwise((Void) null)
            .compose(v -> dao.createDatamart(EXPECTED_DTM))
            .compose(v -> dao.createDatamart(EXPECTED_DTM))
            .onSuccess(s -> testContext.completeNow())
            .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
    }

    @Test
    void datamartNotExists() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.deleteDatamart(EXPECTED_DTM)
            .otherwise((Void) null)
            .compose(v -> dao.getDatamart(EXPECTED_DTM))
            .onSuccess(s -> testContext.completeNow())
            .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
    }
}