package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.MetaStorageGeneratorService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MetaStorageGeneratorServiceImpl implements MetaStorageGeneratorService {

    private final ServiceDbFacade serviceDbFacade;

    @Autowired
    public MetaStorageGeneratorServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.serviceDbFacade = serviceDbFacade;
    }

    @Override
    public void save(DdlRequestContext context, Handler<AsyncResult<Void>> resultHandler) {
        ClassTable classTable = context.getRequest().getClassTable();
        createDatamart(classTable.getSchema(), createDatamartHandler -> {
            if (createDatamartHandler.succeeded()) {
                checkAndCreateTable(classTable.getName(), createDatamartHandler.result(), createTableHandler -> {
                    if (createTableHandler.succeeded()) {
                        createAttributes(createTableHandler.result(), classTable.getFields(), createAttrsHandler -> {
                            if (createAttrsHandler.succeeded()) {
                                resultHandler.handle(Future.succeededFuture());
                            } else {
                                log.debug("Ошибка генерации атрибутов", createAttrsHandler.cause());
                                resultHandler.handle(Future.failedFuture(createAttrsHandler.cause()));
                            }
                        });
                    } else {
                        log.debug("Ошибка генерации таблицы", createTableHandler.cause());
                        resultHandler.handle(Future.failedFuture(createTableHandler.cause()));
                    }
                });
            } else {
                log.debug("Ошибка генерации метаданных", createDatamartHandler.cause());
                resultHandler.handle(Future.failedFuture(createDatamartHandler.cause()));
            }
        });
    }

    private void createAttributes(Long entityId, List<ClassField> fields, Handler<AsyncResult<Void>> resultHandler) {
        List<Future> futures = fields.stream().map(it -> Future.future(p -> createAttribute(entityId, it, ar -> {
            if (ar.succeeded()) {
                p.complete();
            } else {
                p.fail(ar.cause());
            }
        }))).collect(Collectors.toList());
        CompositeFuture.all(futures).onComplete(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void createAttribute(Long entityId, ClassField field, Handler<AsyncResult<Void>> handler) {
        serviceDbFacade.getServiceDbDao().getAttributeTypeDao().findTypeIdByDatamartName(field.getType().name(), ar1 -> {
            if (ar1.succeeded()) {
                serviceDbFacade.getServiceDbDao().getAttributeDao().insertAttribute(entityId, field, ar1.result(), ar2 -> {
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
        serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(datamart, ar1 -> {
            if (ar1.failed()) {
                serviceDbFacade.getServiceDbDao().getDatamartDao().insertDatamart(datamart, ar2 -> {
                    if (ar2.succeeded()) {
                        serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(datamart, resultHandler);
                    } else {
                        resultHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                resultHandler.handle(Future.succeededFuture(ar1.result()));
            }
        });
    }

    private void checkAndCreateTable(String table, Long datamartId, Handler<AsyncResult<Long>> handler) {
        checkNotExistsView(table, datamartId)
                .compose(v -> createTable(table, datamartId))
                .onComplete(handler);
    }

    private Future<Long> createTable(String table, Long datamartId) {
        return Future.future(handler -> serviceDbFacade.getServiceDbDao().getEntityDao().findEntity(datamartId, table, findEntityHandler -> {
            if (findEntityHandler.failed()) {
                log.trace("Вставка сущности {}: {}", datamartId, table);
                insertEntity(table, datamartId)
                        .onSuccess(s -> handler.handle(Future.succeededFuture(s)))
                        .onFailure(f -> handler.handle(Future.failedFuture(f)));
            } else {
                log.trace("Очистка атрибутов для {}: {}", datamartId, table);
                serviceDbFacade.getServiceDbDao().getAttributeDao().dropAttribute(findEntityHandler.result(), dropAttrHandler -> {
                    if (dropAttrHandler.succeeded()) {
                        log.trace("Очистка сущности {}: {}", datamartId, table);
                        serviceDbFacade.getServiceDbDao().getEntityDao().dropEntity(datamartId, table)
                                .compose(v -> insertEntity(table, datamartId))
                                .onComplete(handler);
                    } else {
                        log.error("Ошибка очистки атрибута(dropAttribute)", dropAttrHandler.cause());
                        handler.handle(Future.failedFuture(dropAttrHandler.cause()));
                    }
                });
            }
        }));
    }

    private Future<Void> checkNotExistsView(String viewName, Long datamartId) {
        return Future.future(p -> serviceDbFacade.getServiceDbDao().getViewServiceDao().existsView(viewName, datamartId, ar -> {
            if (ar.succeeded()) {
                if (ar.result()) {
                    String failureMessage = String.format(
                            "View exists by viewName [%s] an datamartId [%d]"
                            , viewName
                            , datamartId);
                    p.handle(Future.failedFuture(failureMessage));
                } else {
                    p.handle(Future.succeededFuture());
                }
            } else {
                p.fail(ar.cause());
            }
        }));
    }

    private Future<Long> insertEntity(String table, Long datamartId) {
        Promise<Long> promise = Promise.promise();
        serviceDbFacade.getServiceDbDao().getEntityDao().insertEntity(datamartId, table, ar1 -> {
            if (ar1.succeeded()) {
                serviceDbFacade.getServiceDbDao().getEntityDao().findEntity(datamartId, table, ar2 -> {
                    if (ar2.succeeded()) {
                        promise.complete(ar2.result());
                    } else {
                        log.error("Не удалось вставить сущность {}", table, ar2.cause());
                        promise.fail(ar2.cause());
                    }
                });
            } else {
                log.error("Ошибка вставки сущности {}", table, ar1.cause());
                promise.fail(ar1.cause());
            }
        });
        return promise.future();
    }
}
