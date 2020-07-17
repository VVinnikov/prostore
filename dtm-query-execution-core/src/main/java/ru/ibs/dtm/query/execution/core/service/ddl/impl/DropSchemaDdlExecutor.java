package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.service.DatabaseSynchronizeService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.List;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaDdlExecutor extends DropTableDdlExecutor {

    public DropSchemaDdlExecutor(
            MetadataFactory<DdlRequestContext> metadataFactory,
            DatabaseSynchronizeService databaseSynchronizeService,
            MariaProperties mariaProperties,
            ServiceDbFacade serviceDbFacade) {
        super(metadataFactory, databaseSynchronizeService, mariaProperties, serviceDbFacade);
    }


    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        context.getRequest().getQueryRequest().setDatamartMnemonic(sqlNodeName);
        dropDatamart(context, sqlNodeName, handler);
    }

    private void dropDatamart(DdlRequestContext context, String datamartName, Handler<AsyncResult<QueryResult>> handler) {
        serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(datamartName, datamartResult -> {
            if (datamartResult.succeeded()) {
                serviceDbFacade.getServiceDbDao().getEntityDao().getEntitiesMeta(datamartName, entitiesMetaResult -> {
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
                                        serviceDbFacade.getServiceDbDao().getDatamartDao().dropDatamart(datamartResult.result(), ar2 -> {
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

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }
}
