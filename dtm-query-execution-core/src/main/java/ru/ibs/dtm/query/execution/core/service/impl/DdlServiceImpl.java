package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.DatamartEntity;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.service.DatabaseSynchronizeService;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;

import java.util.List;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.CREATE_SCHEMA;
import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.DROP_SCHEMA;

@Slf4j
@Service("coreDdlService")
public class DdlServiceImpl implements DdlService<QueryResult> {

    private final ServiceDao serviceDao;
    private final CalciteDefinitionService calciteDefinitionService;
    private final DatabaseSynchronizeService databaseSynchronizeService;
    private final MetadataFactory<DdlRequestContext> metadataFactory;
    private final MariaProperties mariaProperties;
    private final Vertx vertx;

    @Autowired
    public DdlServiceImpl(ServiceDao serviceDao,
                          CalciteDefinitionService calciteDefinitionService,
                          DatabaseSynchronizeService databaseSynchronizeService,
                          MetadataFactory<DdlRequestContext> metadataFactory, MariaProperties mariaProperties,
                          @Qualifier("coreVertx") Vertx vertx
    ) {
        this.serviceDao = serviceDao;
        this.calciteDefinitionService = calciteDefinitionService;
        this.databaseSynchronizeService = databaseSynchronizeService;
        this.metadataFactory = metadataFactory;
        this.mariaProperties = mariaProperties;
        this.vertx = vertx;
    }

    @Override
    public void execute(DdlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        vertx.executeBlocking(it -> {
            try {
                SqlNode node = calciteDefinitionService.processingQuery(context.getRequest().getQueryRequest().getSql());
                it.complete(node);
            } catch (Exception e) {
                log.error("Ошибка парсинга запроса", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                execute(context, asyncResultHandler, ar);
            } else {
                log.debug("Ошибка исполнения", ar.cause());
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void execute(DdlRequestContext context, Handler<AsyncResult<QueryResult>> handler, AsyncResult<Object> ar) {
        if (ar.result() instanceof SqlDdl) {
            SqlDdl sqlDdl = ((SqlDdl) ar.result());
            String sqlNodeName = sqlDdl.getOperandList().stream().filter(t -> t instanceof SqlIdentifier).findFirst().get().toString();

            switch (sqlDdl.getKind()) {
                case CREATE_SCHEMA:
                    createSchema(context, handler, sqlNodeName);
                    break;
                case DROP_SCHEMA:
                    dropSchema(context, handler, sqlNodeName);
                    break;
                case CREATE_TABLE:
                    createTable(context, handler, sqlNodeName);
                    break;
                case DROP_TABLE:
                    dropTable(context, handler, sqlNodeName);
                    break;
                case DEFAULT:
                    log.error("Не поддерживаемый тип DDL запроса");
                    handler.handle(Future.failedFuture(String.format("Не поддерживаемый тип DDL запроса [%s]", context)));
            }
        } else {
            log.error("Не поддерживаемый тип запроса");
            handler.handle(Future.failedFuture(String.format("Не поддерживаемый тип запроса [%s]", context)));
        }
    }

    private void dropTable(DdlRequestContext context, Handler<AsyncResult<QueryResult>> handler, String sqlNodeName) {
        String schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        String table = getTableName(sqlNodeName);
        context.getRequest().getQueryRequest().setDatamartMnemonic(schema);
        dropTable(context, table, containsIfExistsCheck(context.getRequest().getQueryRequest().getSql()), handler);
    }

    private String getTableName(String sqlNodeName) {
        int indexComma = sqlNodeName.indexOf(".");
        return sqlNodeName.substring(indexComma + 1);
    }

    private String getSchemaName(QueryRequest request, String sqlNodeName) {
        int indexComma = sqlNodeName.indexOf(".");
        return indexComma == -1 ? request.getDatamartMnemonic() : sqlNodeName.substring(0, indexComma);
    }

    private void dropSchema(DdlRequestContext context, Handler<AsyncResult<QueryResult>> handler, String sqlNodeName) {
        context.getRequest().getQueryRequest().setDatamartMnemonic(sqlNodeName);
        dropDatamart(context, sqlNodeName, handler);
    }

    private void createSchema(DdlRequestContext context, Handler<AsyncResult<QueryResult>> handler, String sqlNodeName) {
        context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
        context.setDdlType(CREATE_SCHEMA);
        metadataFactory.apply(context, result -> {
            if (result.succeeded()) {
                createDatamart(sqlNodeName, handler);
            } else {
                handler.handle(Future.failedFuture(result.cause()));
            }
        });
    }

    private QueryRequest replaceDatabaseInSql(QueryRequest request) {
        String sql = request.getSql().replaceAll("(?i) database", " schema");
        request.setSql(sql);
        return request;
    }

    //TODO проверка наличия if exists

    /**
     * Проверка, что запрос содержит IF EXISTS
     *
     * @param sql - исходный запрос
     * @return - содержит IF EXISTS
     */
    private boolean containsIfExistsCheck(String sql) {
        return sql.toLowerCase().contains("if exists");
    }

    private void createTable(DdlRequestContext context, Handler<AsyncResult<QueryResult>> handler, String sqlNodeName) {
        String schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        QueryRequest request = context.getRequest().getQueryRequest();
        request.setDatamartMnemonic(schema);

        String tableWithSchema = SqlPreparer.getTableWithSchema(mariaProperties.getOptions().getDatabase(), sqlNodeName);
        String sql = SqlPreparer.replaceQuote(SqlPreparer.replaceTableInSql(request.getSql(), tableWithSchema));
        serviceDao.executeUpdate(sql, ar2 -> {
            if (ar2.succeeded()) {
                databaseSynchronizeService.putForRefresh(
                        context,
                        tableWithSchema,
                        true, ar3 -> {
                            if (ar3.succeeded()) {
                                handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                            } else {
                                log.error("Ошибка синхронизации {}", tableWithSchema, ar3.cause());
                                handler.handle(Future.failedFuture(ar3.cause()));
                            }
                        });
            } else {
                log.error("Ошибка исполнения запроса {}", sql, ar2.cause());
                handler.handle(Future.failedFuture(ar2.cause()));
            }
        });
    }


    private void createDatamart(String datamartName, Handler<AsyncResult<QueryResult>> handler) {
        serviceDao.findDatamart(datamartName, datamartResult -> {
            if (datamartResult.succeeded()) {
                log.error("База данных {} уже существует", datamartName);
                handler.handle(Future.failedFuture(String.format("База данных [%s] уже существует", datamartName)));
            } else {
                serviceDao.insertDatamart(datamartName, insertResult -> {
                    if (insertResult.succeeded()) {
                        log.debug("Создана новая витрина {}", datamartName);
                        handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                    } else {
                        log.error("Ошибка при создании витрины {}", datamartName, insertResult.cause());
                        handler.handle(Future.failedFuture(insertResult.cause()));
                    }
                });
            }
        });
    }

    private void dropTable(DdlRequestContext context, String tableName, boolean ifExists, Handler<AsyncResult<QueryResult>> handler) {
        serviceDao.findDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic(), datamartResult -> {
            if (datamartResult.succeeded()) {
                serviceDao.findEntity(datamartResult.result(), tableName, entityResult -> {
                    if (entityResult.succeeded()) {
                        databaseSynchronizeService.removeTable(context, datamartResult.result(), tableName, removeResult -> {
                            if (removeResult.succeeded()) {
                                handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                            } else {
                                handler.handle(Future.failedFuture(removeResult.cause()));
                            }
                        });
                    } else {
                        if (ifExists) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                            return;
                        }
                        final String msg = "Логической таблицы " + tableName + " не существует (не найдена сущность)";
                        log.error(msg);
                        handler.handle(Future.failedFuture(msg));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(datamartResult.cause()));
            }
        });
    }

    private void dropDatamart(DdlRequestContext context, String datamartName, Handler<AsyncResult<QueryResult>> handler) {
        serviceDao.findDatamart(datamartName, datamartResult -> {
            if (datamartResult.succeeded()) {
                serviceDao.getEntitiesMeta(datamartName, entitiesMetaResult -> {
                    if (entitiesMetaResult.succeeded()) {
                        //удаляем все таблицы
                        dropAllTables(entitiesMetaResult.result(), resultTableDelete -> {
                            if (resultTableDelete.succeeded()) {
                                //удаляем физическую витрину
                                context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
                                context.setDdlType(DROP_SCHEMA);
                                metadataFactory.apply(context, result -> {
                                    if (result.succeeded()) {
                                        //удаляем логическую витрину
                                        serviceDao.dropDatamart(datamartResult.result(), ar2 -> {
                                            if (ar2.succeeded()) {
                                                handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                                            } else {
                                                handler.handle(Future.failedFuture(ar2.cause()));
                                            }
                                        });
                                    } else {
                                        handler.handle(Future.failedFuture(result.cause()));
                                    }
                                });
                            } else {
                                handler.handle(Future.failedFuture(resultTableDelete.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(entitiesMetaResult.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(datamartResult.cause()));
            }
        });

    }

    /**
     * Запуск асинхронного вызова удаления таблиц
     *
     * @param entities - список таблиц на удаление
     * @param handler  - обработчик
     */
    private void dropAllTables(List<DatamartEntity> entities, Handler<AsyncResult<QueryResult>> handler) {
        if (CollectionUtils.isEmpty(entities)) {
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
        } else {
            dropTableChain(entities, 0, handler);
        }
    }

    private void dropTableChain(List<DatamartEntity> entities, int pos, Handler<AsyncResult<QueryResult>> handler) {
        if (pos >= entities.size()) {
            handler.handle(Future.failedFuture("Неправильно переданны входные параметры для удаления таблиц"));
            return;
        }
        DatamartEntity entity = entities.get(pos);
        QueryRequest requestDeleteTable = new QueryRequest();
        requestDeleteTable.setDatamartMnemonic(entity.getDatamartMnemonic());
        requestDeleteTable.setSql("DROP TABLE IF EXISTS " + entity.getDatamartMnemonic() + "." + entity.getMnemonic());
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(requestDeleteTable));
        dropTable(context, entity.getMnemonic(), true,
                ar -> {
                    if (ar.succeeded()) {
                        if (pos + 1 < entities.size()) {
                            dropTableChain(entities, pos + 1, handler);
                        } else {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        }
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
