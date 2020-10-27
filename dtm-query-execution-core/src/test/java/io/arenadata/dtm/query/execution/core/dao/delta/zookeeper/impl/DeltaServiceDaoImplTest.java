package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl;

import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.configuration.properties.ZookeeperProperties;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.impl.*;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.DeltaAlreadyStartedException;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.DeltaNotFinishedException;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.TableBlockedException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class DeltaServiceDaoImplTest {
    public static final String ENV_NAME = "test";
    public static final String DATAMART = "dtm";
    public static final String BAD_DTM = "bad_dtm";
    private TestingServer testingServer;
    private DeltaServiceDaoImpl dao;
    private HotDelta hotDelta;

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
        ZookeeperConnectionProvider manager = new ZookeeperConnectionProviderImpl(properties, ENV_NAME);
        ZookeeperExecutor executor = new ZookeeperExecutorImpl(manager, Vertx.vertx());
        DatamartDao datamartDao = new DatamartDaoImpl(executor, ENV_NAME);
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
        dao.addExecutor(new GetDeltaWriteOperationsExecutorImpl(executor, ENV_NAME));
        val testContext = new VertxTestContext();
        datamartDao.createDatamart(DATAMART)
            .onSuccess(r -> testContext.completeNow())
            .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void fullSuccess() throws InterruptedException {
        val testContext = new VertxTestContext();
        List<Long> sysCns = new ArrayList<>();
        val expectedTime = LocalDateTime.now();
        val expectedDelta = OkDelta.builder()
            .deltaDate(expectedTime)
            .deltaNum(1)
            .cnFrom(5)
            .cnTo(15)
            .build();
        OkDelta[] actualDeltas = new OkDelta[2];
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl0"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl1"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl2"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl3"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl4"))).map(sysCns::add)
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(2)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(3)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(4)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(0)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(1)))
            .compose(r -> dao.getDeltaHot(DATAMART))
            .map(r -> {
                log.info("" + r);
                return r;
            })
            .compose(r -> dao.writeDeltaHotSuccess(DATAMART, LocalDateTime.now().minusHours(1)))
            .compose(r -> dao.getDeltaOk(DATAMART))
            .map(r -> {
                log.info("" + r);
                sysCns.clear();
                return r;
            })
            .compose(r -> dao.writeNewDeltaHot(DATAMART))
            .compose(r -> dao.getDeltaHot(DATAMART))
            .map(r -> {
                log.info("" + r);
                return r;
            })
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl0"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl1"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl2"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl3"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl4"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl5"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl6"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl7"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl8"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl9"))).map(sysCns::add)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl10"))).map(sysCns::add)
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(2)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(3)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(4)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(0)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(1)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(5)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(6)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(7)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(10)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(8)))
            .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(9)))
            .compose(r -> dao.getDeltaHot(DATAMART))
            .map(r -> {
                log.info("" + r);
                return r;
            })
            .compose(r -> dao.writeDeltaHotSuccess(DATAMART, expectedTime))
            .compose(r -> dao.getDeltaOk(DATAMART))
            .map(r -> {
                log.info("" + r);
                actualDeltas[0] = r;
                return r;
            })
            .compose(r -> dao.writeNewDeltaHot(DATAMART))
            .compose(r -> dao.writeDeltaHotSuccess(DATAMART, LocalDateTime.now().plusHours(1)))
            .compose(r -> dao.getDeltaByDateTime(DATAMART, LocalDateTime.now()))
            .onSuccess(r -> {
                actualDeltas[1] = r;
                log.info("result: [{}]", r);
                testContext.completeNow();
            })
            .onFailure(error -> {
                log.error("error", error);
                testContext.failNow(error);
            });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
        assertEquals(expectedDelta, actualDeltas[0]);
        assertEquals(expectedDelta, actualDeltas[1]);
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
        assertTrue(testContext.causeOfFailure() instanceof DeltaAlreadyStartedException);
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
                DeltaWriteOpRequest operation = getOpRequest("tbl1");
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

    @Test
    public void writeDeltaHotSuccessNotFinishedOperation() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl0")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl1")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl2")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl3")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl4")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl5")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl6")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl7")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl8")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl9")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl10")))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 2L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 3L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 4L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 0L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 1L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 5L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 6L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 10L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 8L))
            .compose(r -> dao.writeOperationSuccess(DATAMART, 9L))
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
        assertTrue(testContext.causeOfFailure() instanceof DeltaNotFinishedException);
    }

    @Test
    public void writeOperationError() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl0")))
            .compose(r -> dao.writeOperationError(DATAMART, 0L))
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
    public void writeOperationSuccessTableBlocked() throws InterruptedException {
        val testContext = new VertxTestContext();
        dao.writeNewDeltaHot(DATAMART)
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl1")))
            .compose(r -> dao.writeNewOperation(getOpRequest("tbl1")))
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
        assertTrue(testContext.causeOfFailure() instanceof TableBlockedException);
    }

    @Test
    void getWriteOpListTest() throws InterruptedException {
        val testContext = new VertxTestContext();
        List<Long> sysCns = new ArrayList<>();
        final List<String> tables = Arrays.asList("tbl0", "tbl1", "tbl2", "tbl3", "tbl4");
        final Map<String, Long> expectedCnMap = new HashMap<>();
        expectedCnMap.put(tables.get(0), 5L);
        expectedCnMap.put(tables.get(1), 1L);
        expectedCnMap.put(tables.get(2), 2L);
        expectedCnMap.put(tables.get(3), 3L);
        expectedCnMap.put(tables.get(4), 4L);
        List<DeltaWriteOp> actualWriteOpList = new ArrayList<>();
        dao.writeNewDeltaHot(DATAMART)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(0)))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(1)))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(2)))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(3)))).map(sysCns::add)
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(4)))).map(sysCns::add)
                .compose(r -> dao.writeOperationSuccess(DATAMART, sysCns.get(0)))
                .compose(r -> dao.writeNewOperation(getOpRequest(tables.get(0)))).map(sysCns::add)
                .compose(r -> dao.getDeltaWriteOperations(DATAMART))
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    actualWriteOpList.addAll(r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
        assertFalse(actualWriteOpList.isEmpty());
        actualWriteOpList.forEach(wrOp -> assertEquals(expectedCnMap.get(wrOp.getTableName()), wrOp.getSysCn()));
    }

    @Test
    void getNullWriteOpList() throws InterruptedException {
        val testContext = new VertxTestContext();
        List<List<DeltaWriteOp>> result = new ArrayList<>();
        dao.getDeltaWriteOperations(DATAMART)
                .onSuccess(r -> {
                    log.info("result: [{}]", r);
                    result.add(r);
                    testContext.completeNow();
                })
                .onFailure(error -> {
                    log.error("error", error);
                    testContext.failNow(error);
                });
        assertThat(testContext.awaitCompletion(120, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.completed());
        assertNull(result.get(0));
    }

    private DeltaWriteOpRequest getOpRequest(String tableName) {
        return DeltaWriteOpRequest.builder()
            .tableNameExt(tableName + "_ext")
            .datamart(DATAMART)
            .tableName(tableName)
            .query("select 1")
            .build();
    }

}