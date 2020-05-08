package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.service.MetaStorageGeneratorService;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MetaStorageGeneratorServiceImpl implements MetaStorageGeneratorService {

  private final ServiceDao serviceDao;

  @Autowired
  public MetaStorageGeneratorServiceImpl(ServiceDao serviceDao) {
    this.serviceDao = serviceDao;
  }

  @Override
  public void save(ClassTable classTable, Handler<AsyncResult<Void>> resultHandler) {
    createDatamart(classTable.getSchema(), ar1 -> {
      if (ar1.succeeded()) {
        createTable(classTable.getName(), ar1.result(), ar2 -> {
          if (ar2.succeeded()) {
            createAttributes(ar2.result(), classTable.getFields(), ar3 -> {
              if (ar3.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
              } else {
                log.debug("Ошибка генерации атрибутов", ar3.cause());
                resultHandler.handle(Future.failedFuture(ar3.cause()));
              }
            });
          } else {
            log.debug("Ошибка генерации таблицы", ar2.cause());
            resultHandler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      } else {
        log.debug("Ошибка генерации метаданных", ar1.cause());
        resultHandler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }

  private void createAttributes(Long entityId, List<ClassField> fields, Handler<AsyncResult<Void>> resultHandler) {
    List<Future> futures = fields.stream().map(it -> {
      return Future.future(p -> createAttribute(entityId, it, ar -> {
        if (ar.succeeded()) {
          p.complete();
        } else {
          p.fail(ar.cause());
        }
      }));
    }).collect(Collectors.toList());
    CompositeFuture.all(futures).onComplete(ar -> {
      if (ar.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        resultHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void createAttribute(Long entityId, ClassField field, Handler<AsyncResult<Void>> handler) {
    serviceDao.selectType(field.getType().name(), ar1 -> {
      if (ar1.succeeded()) {
        serviceDao.insertAttribute(entityId, field.getName(), ar1.result(), field.getSize(), ar2 -> {
          if (ar2.succeeded()) {
            handler.handle(Future.succeededFuture());
          } else {
            handler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      } else {
        handler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }

  private void createDatamart(String datamart, Handler<AsyncResult<Long>> resultHandler) {
    serviceDao.findDatamart(datamart, ar1 -> {
      if (ar1.failed()) {
        serviceDao.insertDatamart(datamart, ar2 -> {
          if (ar2.succeeded()) {
            serviceDao.findDatamart(datamart, resultHandler);
          } else {
            resultHandler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      } else {
        resultHandler.handle(Future.succeededFuture(ar1.result()));
      }
    });
  }

  private void createTable(String table, Long datamartId, Handler<AsyncResult<Long>> resultHandler) {
    serviceDao.findEntity(datamartId, table, ar1 -> {
      if (ar1.failed()) {
        log.trace("Вставка сущности {}: {}", datamartId, table);
        insertEntity(table, datamartId, resultHandler);
      } else {
        log.trace("Очистка сущности {}: {}", datamartId, table);
        serviceDao.dropEntity(datamartId, table, ar2 -> {
          if (ar2.succeeded()) {
            log.trace("Очистка атрибутов для {}: {}", datamartId, table);
            serviceDao.dropAttribute(ar1.result(), ar3 -> {
              if (ar3.succeeded()) {
                insertEntity(table, datamartId, resultHandler);
              } else {
                log.error("Ошибка очистки атрибута(dropAttribute)", ar3.cause());
                resultHandler.handle(Future.failedFuture(ar3.cause()));
              }
            });
          } else {
            resultHandler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      }
    });
  }

  private void insertEntity(String table, Long datamartId, Handler<AsyncResult<Long>> handler) {
    serviceDao.insertEntity(datamartId, table, ar1 -> {
      if (ar1.succeeded()) {
        serviceDao.findEntity(datamartId, table, ar2 -> {
          if (ar2.succeeded()) {
            handler.handle(Future.succeededFuture(ar2.result()));
          } else {
            log.error("Не удалось вставить сущность {}", table, ar2.cause());
            handler.handle(Future.failedFuture(ar1.cause()));
          }
        });
      } else {
        log.error("Ошибка вставки сущности {}", table, ar1.cause());
        handler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }
}
