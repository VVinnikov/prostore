package ru.ibs.dtm.query.execution.core.dao.impl;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.execution.core.CoreTestConfiguration;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadQueryRecord;

import java.util.*;
import java.util.concurrent.TimeUnit;

@ActiveProfiles("test")
@SpringBootTest(classes = CoreTestConfiguration.class)
@ExtendWith(VertxExtension.class)
class ServiceDaoImplIT {

    @Autowired
    ServiceDbFacade serviceDbFacade;
    private String datamart = "test";
    private Long datamartId = 2L;
    private String entity = "entity_test";
    private Long entityId = 8L;
    private String attrName = "attr_test";
    private DownloadExtTableRecord downloadExtTableRecord;
    private List<Long> deltas;

    @Test
    void insertDatamart(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getDatamartDao().createDatamart(datamart)
            .onFailure(testContext::failNow)
            .onSuccess(s -> testContext.completeNow());
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void findDatamart(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getDatamartDao().getDatamart(datamart)
            .onFailure(testContext::failNow)
            .onSuccess(s -> testContext.completeNow());
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void insertEntity(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getEntityDao().createEntity(Entity.builder()
            .name(entity)
            .schema(datamart)
            .build())
            .onFailure(testContext::failNow)
            .onSuccess(s -> testContext.completeNow());
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void findEntity(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getEntityDao().getEntity(datamart, entity)
            .onFailure(testContext::failNow)
            .onSuccess(s -> testContext.completeNow());
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void dropEntity(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getEntityDao().deleteEntity(datamart, entity)
            .onFailure(testContext::failNow)
            .onSuccess(s -> testContext.completeNow());
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void dropDatamart(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getDatamartDao().deleteDatamart(datamart)
            .onFailure(testContext::failNow)
            .onSuccess(s -> testContext.completeNow());
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void findDownloadExternalTable(VertxTestContext testContext) throws Throwable {
        String externalTable = "tblExt";
        serviceDbFacade.getEddlServiceDao().getDownloadExtTableDao().findDownloadExternalTable(datamart, externalTable, ar -> {
            if (ar.succeeded()) {
                downloadExtTableRecord = ar.result();
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
        Assertions.assertEquals(externalTable, downloadExtTableRecord.getTableName());
        //Assertions.assertEquals(datamart, downloadExtTableRecord.getDatamart());
    }

    @Test
    void insertDownloadQuery(VertxTestContext testContext) throws Throwable {
        DownloadQueryRecord record = new DownloadQueryRecord(UUID.randomUUID().toString(), 1L,
            "ext_table", "select 1", 0);
        serviceDbFacade.getEddlServiceDao().getDownloadQueryDao().insertDownloadQuery(record, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void dropTable(VertxTestContext testContext) throws Throwable {
        Entity entity = new Entity("dtmservice.test_doc", Collections.emptyList());
        String createScript = String.format("create table if not exists %s (id integer not null, gnr varchar(25) not null)", entity.getNameWithSchema());
        serviceDbFacade.getDdlServiceDao().executeUpdate(createScript, ar1 -> {
            if (ar1.succeeded()) {
                serviceDbFacade.getDdlServiceDao().dropTable(entity, ar2 -> {
                    if (ar2.succeeded()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar2.cause());
                    }
                });
            } else {
                testContext.failNow(ar1.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }
}
