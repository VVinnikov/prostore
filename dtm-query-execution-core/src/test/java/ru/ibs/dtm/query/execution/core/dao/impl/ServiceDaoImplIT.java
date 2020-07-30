package ru.ibs.dtm.query.execution.core.dao.impl;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
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

    private String datamart = "test";
    private Long datamartId = 2L;
    private String entity = "entity_test";
    private Long entityId = 8L;
    private String attrName = "attr_test";
    private DownloadExtTableRecord downloadExtTableRecord;
    private List<Long> deltas;

    @Autowired
    ServiceDbFacade serviceDbFacade;

    @Test
    void insertDatamart(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getDatamartDao().insertDatamart(datamart, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void findDatamart(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(datamart, ar -> {
            if (ar.succeeded()) {
                datamartId = ar.result();
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void insertEntity(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getEntityDao().insertEntity(datamartId, entity, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void findEntity(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getEntityDao().findEntity(datamartId, entity, ar -> {
            if (ar.succeeded()) {
                entityId = ar.result();
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void insertAttribute(VertxTestContext testContext) throws Throwable {
        ClassField cf = new ClassField(0, attrName, null, null, null, null, null);
        serviceDbFacade.getServiceDbDao().getAttributeDao().insertAttribute(entityId, cf, 1, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void dropAttribute(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getAttributeDao().dropAttribute(entityId, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void dropEntity(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getEntityDao().dropEntity(datamartId, entity, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void dropDatamart(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getDatamartDao().dropDatamart(datamartId, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void selectType(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getServiceDbDao().getAttributeTypeDao().findTypeIdByTypeMnemonic("varchar", ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void getDeltaOnDateTime(VertxTestContext testContext) throws Throwable {
        serviceDbFacade.getDeltaServiceDao().getDeltaOnDateTime(new ActualDeltaRequest("test_datamart", "2020-03-26 11:30:26", false), ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    void getDeltasOnDateTimes(VertxTestContext testContext) throws Throwable {
        final List<ActualDeltaRequest> requests = Arrays.asList(
                new ActualDeltaRequest("dm2", "2020-04-15 07:00:00", false),
                new ActualDeltaRequest("dm3", "2020-04-01 07:00:00", false),
                new ActualDeltaRequest("dm2", "2020-03-01 07:00:00", false),
                new ActualDeltaRequest("dmX", "2020-04-01 07:00:00", false)
        );
        serviceDbFacade.getDeltaServiceDao().getDeltasOnDateTimes(requests, ar -> {
            if (ar.succeeded()) {
                deltas = ar.result();
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
        Assertions.assertEquals(requests.size(), deltas.size());
    }

    @Test
    void getDeltasOnDateTimesWithEmptyRequests(VertxTestContext testContext) throws Throwable {
        final List<ActualDeltaRequest> requests = new ArrayList<>();
        serviceDbFacade.getDeltaServiceDao().getDeltasOnDateTimes(requests, ar -> {
            if (ar.succeeded()) {
                deltas = ar.result();
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
        Assertions.assertEquals(requests.size(), deltas.size());
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
        ClassTable classTable = new ClassTable("dtmservice.test_doc", Collections.emptyList());
        String createScript = String.format("create table if not exists %s (id integer not null, gnr varchar(25) not null)", classTable.getNameWithSchema());
        serviceDbFacade.getDdlServiceDao().executeUpdate(createScript, ar1 -> {
            if (ar1.succeeded()) {
                serviceDbFacade.getDdlServiceDao().dropTable(classTable, ar2 -> {
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
