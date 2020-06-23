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
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.DownloadExtTableRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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
  ServiceDao serviceDao;

  @Test
  void insertDatamart(VertxTestContext testContext) throws Throwable {
    serviceDao.insertDatamart(datamart, ar -> {
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
    serviceDao.findDatamart(datamart, ar -> {
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
    serviceDao.insertEntity(datamartId, entity, ar -> {
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
    serviceDao.findEntity(datamartId, entity, ar -> {
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
    ClassField cf = new ClassField(attrName, null, null, null, null, null);
    serviceDao.insertAttribute(entityId, cf, 1,  ar -> {
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
    serviceDao.dropAttribute(entityId, ar -> {
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
    serviceDao.dropEntity(datamartId, entity)
            .onSuccess(s -> testContext.completeNow())
            .onFailure(testContext::failNow);
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

  @Test
  void dropDatamart(VertxTestContext testContext) throws Throwable {
    serviceDao.dropDatamart(datamartId, ar -> {
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
    serviceDao.selectType("varchar", ar -> {
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
    serviceDao.getDeltaOnDateTime(new ActualDeltaRequest("test_datamart", "2020-03-26 11:30:26"), ar -> {
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
      new ActualDeltaRequest("dm2", "2020-04-15 07:00:00"),
      new ActualDeltaRequest("dm3", "2020-04-01 07:00:00"),
      new ActualDeltaRequest("dm2", "2020-03-01 07:00:00"),
      new ActualDeltaRequest("dmX", "2020-04-01 07:00:00")
    );
    serviceDao.getDeltasOnDateTimes(requests, ar -> {
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
    serviceDao.findDownloadExternalTable(datamart, externalTable, ar -> {
      if (ar.succeeded()) {
        downloadExtTableRecord = ar.result();
        testContext.completeNow();
      } else {
        testContext.failNow(ar.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
    Assertions.assertEquals(externalTable, downloadExtTableRecord.getTableName());
    Assertions.assertEquals(datamart, downloadExtTableRecord.getDatamart());
  }

  @Test
  void insertDownloadQuery(VertxTestContext testContext) throws Throwable {
    serviceDao.insertDownloadQuery(UUID.randomUUID(), 1L, "select 1", ar -> {
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
    serviceDao.executeUpdate(createScript, ar1 -> {
      if (ar1.succeeded()) {
        serviceDao.dropTable(classTable, ar2 -> {
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
