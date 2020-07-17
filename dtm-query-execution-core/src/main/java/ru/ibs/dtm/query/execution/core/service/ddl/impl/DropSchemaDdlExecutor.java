package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.ArrayList;
import java.util.List;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaDdlExecutor extends DropTableDdlExecutor {

    public DropSchemaDdlExecutor(
            MetadataExecutor<DdlRequestContext> metadataExecutor,
            MariaProperties mariaProperties,
            ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            context.getRequest().getQueryRequest().setDatamartMnemonic(sqlNodeName);
            context.setDatamartName(sqlNodeName);
            getDatamart(context)
                    .compose(datamartId -> getDatamartEntities(context, datamartId))
                    .onComplete(ar -> dropTablesAndSchema(context, ar, handler))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error in deleting datamart!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Long> getDatamart(DdlRequestContext context) {
        return Future.future((Promise<Long> promise) ->
                serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(context.getDatamartName(), promise));
    }

    private Future<List<DatamartEntity>> getDatamartEntities(DdlRequestContext context, Long datamartId) {
        return Future.future((Promise<List<DatamartEntity>> promise) -> {
            context.setDatamartId(datamartId);
            serviceDbFacade.getServiceDbDao().getEntityDao().getEntitiesMeta(context.getDatamartName(), promise);
        });
    }

    private void dropTablesAndSchema(DdlRequestContext context, AsyncResult<List<DatamartEntity>> ar,
                                     Handler<AsyncResult<QueryResult>> handler) {
        if (ar.succeeded()) {
            //удаляем все таблицы
            List<DatamartEntity> entities = ar.result();
            List<Future> dropTableFutures = new ArrayList<>();
            entities.forEach(e -> dropTableFutures.add(createDropTableFuture(e)));
            CompositeFuture.join(dropTableFutures)
                    .onComplete(dr -> dropDatamart(context, dr, handler))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } else {
            handler.handle(Future.failedFuture(ar.cause()));
        }
    }

    private Future<Void> createDropTableFuture(DatamartEntity entity) {
        QueryRequest requestDeleteTable = new QueryRequest();
        requestDeleteTable.setDatamartMnemonic(entity.getDatamartMnemonic());
        requestDeleteTable.setSql("DROP TABLE IF EXISTS " + entity.getDatamartMnemonic() + "." + entity.getMnemonic());
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(requestDeleteTable));
        log.debug("Send drop table request for table [{}] and datamart [{}]", entity.getMnemonic(),
                entity.getDatamartMnemonic());
        return dropTable(context, true);
    }

    private void dropDatamart(DdlRequestContext context, AsyncResult<CompositeFuture> dr,
                              Handler<AsyncResult<QueryResult>> handler) {
        if (dr.succeeded()) {
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(DROP_SCHEMA);
            //удаляем физическую витрину
            metadataExecutor.execute(context, result -> {
                if (result.succeeded()) {
                    log.debug("Deleted schema [{}] in data sources", context.getDatamartName());
                    //удаляем логическую витрину
                    serviceDbFacade.getServiceDbDao().getDatamartDao().dropDatamart(context.getDatamartId(), ar2 -> {
                        if (ar2.succeeded()) {
                            log.debug("Deleted datamart [{}] in metadata", context.getDatamartName());
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
            handler.handle(Future.failedFuture(dr.cause()));
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }
}
