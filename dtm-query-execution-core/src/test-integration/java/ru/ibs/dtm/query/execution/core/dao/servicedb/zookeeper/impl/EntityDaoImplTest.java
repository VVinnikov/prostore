package ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.query.execution.core.configuration.properties.ZookeeperProperties;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import ru.ibs.dtm.query.execution.core.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import ru.ibs.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class EntityDaoImplTest {

    public static final String EXPECTED_DTM = "dtm1";
    public static final String EXPECTED_ENT_NAME1 = "tbl1";
    public static final String EXPECTED_ENT_NAME2 = "tbl2";
    private final DatamartDaoImpl datamartDao;
    private final EntityDaoImpl entityDao;
    private final Entity expectedEntity1;
    private final Entity expectedEntity2;

    public EntityDaoImplTest() {
        ZookeeperExecutor executor = new ZookeeperExecutorImpl(new ZookeeperConnectionProviderImpl(getZookeeperProperties()), Vertx.vertx());
        datamartDao = new DatamartDaoImpl(executor, "test1");
        entityDao = new EntityDaoImpl(executor, "test1");
        expectedEntity1 = getEntity(EXPECTED_ENT_NAME1);
        expectedEntity2 = getEntity(EXPECTED_ENT_NAME2);
    }

    private Entity getEntity(String entityName) {
        return Entity.builder()
            .schema(EntityDaoImplTest.EXPECTED_DTM)
            .name(entityName)
            .fields(getFields())
            .build();
    }

    private List<EntityField> getFields() {
        List<EntityField> fields = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            fields.add(EntityField.builder()
                .accuracy(1)
                .defaultValue("def")
                .name("f" + i)
                .nullable(false)
                .ordinalPosition(1)
                .primaryOrder(2)
                .shardingOrder(3)
                .size(1)
                .type(ColumnType.BIGINT)
                .build());
        }
        return fields;
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
    void createEntity() throws InterruptedException {
        val testContext = new VertxTestContext();
        datamartDao.deleteDatamart(EXPECTED_DTM)
            .otherwise((Void) null)
            .compose(v -> datamartDao.createDatamart(EXPECTED_DTM))
            .compose(v -> entityDao.createEntity(expectedEntity1))
            .compose(v -> entityDao.createEntity(expectedEntity2))
            .compose(v -> entityDao.getEntityNamesByDatamart(EXPECTED_DTM))
            .compose(names -> {
                assertTrue(names.contains(EXPECTED_ENT_NAME1));
                assertTrue(names.contains(EXPECTED_ENT_NAME2));
                return entityDao.deleteEntity(expectedEntity1.getSchema(), expectedEntity1.getName());
            })
            .compose(v -> entityDao.getEntity(expectedEntity2.getSchema(), expectedEntity2.getName()))
            .compose(actualEntity -> {
                assertEquals(expectedEntity2, actualEntity);
                return datamartDao.deleteDatamart(EXPECTED_DTM);
            })
            .onSuccess(s -> testContext.completeNow())
            .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertFalse(testContext.failed());
    }

    @Test
    void entityAlreadyExists() throws InterruptedException {
        val testContext = new VertxTestContext();
        datamartDao.deleteDatamart(EXPECTED_DTM)
            .otherwise((Void) null)
            .compose(v -> datamartDao.createDatamart(EXPECTED_DTM))
            .compose(v -> entityDao.createEntity(expectedEntity1))
            .compose(v -> entityDao.createEntity(expectedEntity1))
            .onSuccess(s -> testContext.completeNow())
            .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
    }

    @Test
    void entityNotExists() throws InterruptedException {
        val testContext = new VertxTestContext();
        datamartDao.deleteDatamart(EXPECTED_DTM)
            .otherwise((Void) null)
            .compose(v -> datamartDao.createDatamart(EXPECTED_DTM))
            .compose(v -> entityDao.getEntity(expectedEntity2.getSchema(), expectedEntity2.getName()))
            .onSuccess(s -> testContext.completeNow())
            .onFailure(testContext::failNow);
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertTrue(testContext.failed());
    }

}