package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.impl;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.execution.core.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.properties.ZookeeperProperties;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl.*;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import ru.ibs.dtm.query.execution.core.service.zookeeper.impl.ZKConnectionProviderImpl;
import ru.ibs.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class DeltaServiceDaoImplTest {
    public static final String ENV_NAME = "test";
    public static final String DATAMART = "dtm";
    public static final String BAD_DTM = "bad_dtm";
    private TestingServer testingServer;
    private DeltaServiceDaoImpl dao;
    private DatamartDao datamartDao;

    public DeltaServiceDaoImplTest() throws Exception {
        new AppConfiguration(null).objectMapper();
    }

    @BeforeEach
    public void before() throws Exception {
        testingServer = new TestingServer(55431, true);
        dao = new DeltaServiceDaoImpl();
        initExecutors(dao);
    }

    @AfterEach
    public void after() throws IOException {
        testingServer.stop();
        testingServer.close();
    }

    private void initExecutors(DeltaServiceDaoImpl dao) throws Exception {
        ZookeeperProperties properties = new ZookeeperProperties();
        properties.setChroot("/arena");
        properties.setConnectionString("localhost:55431");
        properties.setConnectionTimeoutMs(10_000);
        properties.setSessionTimeoutMs(30_000);
        ZKConnectionProviderImpl manager = new ZKConnectionProviderImpl(properties);
        ZookeeperExecutor executor = new ZookeeperExecutorImpl(manager, Vertx.vertx());
        datamartDao = new DatamartDaoImpl(executor, ENV_NAME);
        dao.addExecutor(new DeleteDeltaHotExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new DeleteWriteOperationExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new GetDeltaByDateTimeExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new GetDeltaByNumExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new GetDeltaHotExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new GetDeltaOkExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new WriteDeltaErrorExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new WriteDeltaHotSuccessExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new WriteNewDeltaHotExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new WriteNewOperationExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new WriteOperationErrorExecutorImpl(executor, ENV_NAME));
        dao.addExecutor(new WriteOperationSuccessExecutorImpl(executor, ENV_NAME));
        val testContext = new VertxTestContext();
        datamartDao.createDatamart(DATAMART)
            .onSuccess(r -> testContext.completeNow())
            .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void writeNewDeltaHot() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    public void writeNewDeltaHotBad() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(BAD_DTM)
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());

    }

    @Test
    public void writeNewDeltaHotAlreadyExists() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeNewDeltaHot(DATAMART))
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
    }

    @Test
    public void writeDeltaHotSuccess() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    public void writeDeltaHotSuccessNotStarted() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
            .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
    }

    @Test
    public void writeManyDeltaHotSuccess() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
            .compose(r -> dao.writeNewDeltaHot(DATAMART))
            .compose(r -> dao.writeDeltaHotSuccess(DATAMART))
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    public void writeDeltaError() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeDeltaError(DATAMART, 0L))
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    public void deleteDeltaHot() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.deleteDeltaHot(DATAMART))
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

    @Test
    public void writeNewOperation() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> {
                DeltaWriteOpRequest operation = DeltaWriteOpRequest.builder()
                    .tableNameExt("tbl1_ext")
                    .datamart(DATAMART)
                    .tableName("tbl1")
                    .query("select 1")
                    .build();
                return dao.writeNewOperation(operation);
            })
            .onSuccess(r -> {
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
    }

}
