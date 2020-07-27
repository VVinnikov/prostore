package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.extension.eddl.DropDatabase;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.ArrayList;
import java.util.List;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaDdlExecutor extends DropTableDdlExecutor {

    private final DefinitionService<SqlNode> definitionService;

    public DropSchemaDdlExecutor(
            MetadataExecutor<DdlRequestContext> metadataExecutor,
            MariaProperties mariaProperties,
            ServiceDbFacade serviceDbFacade,
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
        this.definitionService = definitionService;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            String schemaName = ((DropDatabase) context.getQuery()).getName().names.get(0);
            context.getRequest().getQueryRequest().setDatamartMnemonic(schemaName);
            context.setDatamartName(schemaName);
            getDatamart(context)
                    .compose(datamartId -> getDatamartEntities(context, datamartId))
                    .compose(entities -> dropDatamartObjects(context, entities))
                    .onSuccess(ar -> dropSchema(context)
                            .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                            .onFailure(fail -> handler.handle(Future.failedFuture(fail))))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error deleting datamart!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Long> getDatamart(DdlRequestContext context) {
        return Future.future((Promise<Long> promise) ->
                serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(context.getDatamartName(), ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        log.error("Error finding datamart [{}]!", context.getDatamartName(), ar.cause());
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<List<DatamartEntity>> getDatamartEntities(DdlRequestContext context, Long datamartId) {
        return Future.future((Promise<List<DatamartEntity>> promise) -> {
            context.setDatamartId(datamartId);
            serviceDbFacade.getServiceDbDao().getEntityDao().getEntitiesMeta(context.getDatamartName(), ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    log.error("Error receiving entities for datamart [{}]!", context.getDatamartName(), ar.cause());
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private Future<QueryResult> dropDatamartObjects(DdlRequestContext context, List<DatamartEntity> entities) {
        return Future.future((Promise<QueryResult> promise) -> {
            List<Future> dropTablesAndViewsFutures = new ArrayList<>();
            dropTablesAndViewsFutures.add(createDropViewsFuture(context));
            entities.forEach(e -> dropTablesAndViewsFutures.add(createDropTableFuture(e)));
            CompositeFuture.join(dropTablesAndViewsFutures)
                    .onSuccess(dr -> promise.complete())
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> createDropViewsFuture(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) ->
                serviceDbFacade.getServiceDbDao().getViewServiceDao().dropViewsByDatamartId(context.getDatamartId(), ar -> {
                    if (ar.succeeded()) {
                        log.debug("Deleted views in datamart [{}]", context.getDatamartName());
                        promise.complete();
                    } else {
                        log.error("Error deleting views in datamart [{}]!", context.getDatamartName(), ar.cause());
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<Void> createDropTableFuture(DatamartEntity entity) {
        String nameWithSchema = entity.getDatamartMnemonic() + "." + entity.getMnemonic();
        //формируем context для удаления физических таблиц
        DdlRequestContext context = createDropTableRequestContext(entity, nameWithSchema);
        log.debug("Send drop table request for table [{}] and datamart [{}]", entity.getMnemonic(),
                entity.getDatamartMnemonic());
        return dropTable(context, true);
    }

    @NotNull
    private DdlRequestContext createDropTableRequestContext(DatamartEntity entity, String nameWithSchema) {
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(createQueryRequest(entity, nameWithSchema)));
        context.getRequest().setClassTable(new ClassTable(nameWithSchema, null));
        context.setDatamartName(entity.getDatamartMnemonic());
        context.setDdlType(DdlType.DROP_TABLE);
        //FIXME redo without re-parsing request
        context.setQuery(definitionService.processingQuery(context.getRequest().getQueryRequest().getSql()));
        return context;
    }

    @NotNull
    private QueryRequest createQueryRequest(DatamartEntity entity, String nameWithSchema) {
        QueryRequest requestDropTable = new QueryRequest();
        requestDropTable.setDatamartMnemonic(entity.getDatamartMnemonic());
        requestDropTable.setSql("DROP TABLE IF EXISTS " + nameWithSchema);
        return requestDropTable;
    }

    private Future<Void> dropSchema(DdlRequestContext context) {
        try {
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(DROP_SCHEMA);
            //удаляем физическую витрину
            return Future.future((Promise<Void> promise) -> metadataExecutor.execute(context, promise))
                    .compose(result -> dropDatamart(context));
        } catch (Exception e) {
            log.error("Error in dropping schema [{}]", context.getDatamartName(), e);
            return Future.failedFuture(e);
        }
    }

    private Future<Void> dropDatamart(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) -> {
            log.debug("Deleted schema [{}] in data sources", context.getDatamartName());
            //удаляем логическую витрину
            serviceDbFacade.getServiceDbDao().getDatamartDao().dropDatamart(context.getDatamartId(), ar -> {
                if (ar.succeeded()) {
                    log.debug("Deleted datamart [{}] from metadata", context.getDatamartName());
                    promise.complete();
                } else {
                    log.error("Error deleting datamart [{}] from metadata!", context.getDatamartName(), ar.cause());
                    promise.fail(ar.cause());
                }
            });
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }
}
