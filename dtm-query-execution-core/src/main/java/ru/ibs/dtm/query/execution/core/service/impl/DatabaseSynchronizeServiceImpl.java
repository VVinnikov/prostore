package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.service.DatabaseSynchronizeService;
import ru.ibs.dtm.query.execution.core.service.MetaStorageGeneratorService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlQueryType.CREATE_TABLE;
import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlQueryType.DROP_TABLE;

@Service
@Slf4j
public class DatabaseSynchronizeServiceImpl implements DatabaseSynchronizeService {

  private MetadataFactory<DdlRequestContext> metadataFactory;
  private MetaStorageGeneratorService generatorService;
  private ServiceDao serviceDao;

  @Autowired
  public DatabaseSynchronizeServiceImpl(MetadataFactory<DdlRequestContext> metadataFactory,
                                        MetaStorageGeneratorService generatorService,
                                        ServiceDao serviceDao) {
    this.metadataFactory = metadataFactory;
    this.generatorService = generatorService;
    this.serviceDao = serviceDao;
  }

  @Override
  public void putForRefresh(QueryRequest request,
                            String table,
                            boolean createTopics,
                            Handler<AsyncResult<Void>> handler) {
    metadataFactory.reflect(table, ar1 -> {
      if (ar1.succeeded()) {
        ClassTable classTable = ar1.result();
        serviceDao.dropTable(classTable, ar2 -> {
          if (ar2.succeeded()) {
            classTable.setSchema(request.getDatamartMnemonic());
            classTable.setNameWithSchema(request.getDatamartMnemonic() + "." + classTable.getName());
            applyMetadata(new DdlRequestContext(new DdlRequest(request, classTable, CREATE_TABLE)), classTable, handler);
          } else {
            log.debug("Ошибка удаления таблицы в сервисной БД", ar2.cause());
            handler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      } else {
        log.debug("Ошибка получения данных о таблицах из сервисной БД", ar1.cause());
        handler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }

  @Override
  public void removeTable(QueryRequest request, Long datamartId, String tableName, Handler<AsyncResult<Void>> handler) {
    metadataFactory.reflect(tableName, ar1 -> {
      if (ar1.succeeded()) {
        ClassTable classTable = ar1.result();
        serviceDao.dropTable(classTable, dropTableResult -> {
          if (dropTableResult.succeeded()) {
            classTable.setSchema(request.getDatamartMnemonic());
            classTable.setNameWithSchema(request.getDatamartMnemonic() + "." + classTable.getName());

            metadataFactory.apply(new DdlRequestContext(new DdlRequest(request, classTable, DROP_TABLE)), result -> {
              if (result.succeeded()) {
                log.trace("Удаление сущности {} из схемы {}", tableName, request.getDatamartMnemonic());
                serviceDao.dropEntity(datamartId, tableName, ar2 -> {
                  if (ar2.succeeded()) {
                    handler.handle(Future.succeededFuture());
                  } else {
                    handler.handle(Future.failedFuture(ar2.cause()));
                  }
                });
              } else {
                handler.handle(Future.failedFuture(result.cause()));
              }
            });
          } else {
            log.debug("Ошибка удаления таблицы в сервисной БД", dropTableResult.cause());
            handler.handle(Future.failedFuture(dropTableResult.cause()));
          }
        });
      } else {
        log.debug("Ошибка получения данных о таблицах из сервисной БД", ar1.cause());
        handler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }

  private void applyMetadata(DdlRequestContext context, ClassTable classTable, Handler<AsyncResult<Void>> handler) {
    metadataFactory.apply(context, ar1 -> {
      if (ar1.succeeded()) {
        generatorService.save(classTable, ar2 -> {
          if (ar2.succeeded()) {
            handler.handle(Future.succeededFuture());
          } else {
            log.debug("Ошибка при генерации метаданных", ar2.cause());
            handler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      } else {
        log.debug("Ошибка при отображение таблицы из сервисной БД", ar1.cause());
        handler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }

}
