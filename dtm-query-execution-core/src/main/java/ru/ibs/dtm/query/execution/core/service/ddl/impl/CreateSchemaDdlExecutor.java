package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.exception.DatamartAlreadyExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.CREATE_SCHEMA;

@Slf4j
@Component
public class CreateSchemaDdlExecutor extends QueryResultDdlExecutor {

    private final DatamartDao datamartDao;

    @Autowired
    public CreateSchemaDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                   ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            String schemaName = ((SqlCreateDatabase) context.getQuery()).getName().names.get(0);
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(CREATE_SCHEMA);
            context.setDatamartName(schemaName);
            createDatamartIfNotExists(context, handler);
        } catch (Exception e) {
            log.error("Error creating datamart!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private void createDatamartIfNotExists(DdlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        datamartDao.existsDatamart(context.getDatamartName())
            .compose(isExists -> isExists ? getDatamarAlreadyExistsFuture(context) : createDatamartInPlugins(context))
            .compose(v -> datamartDao.createDatamart(context.getDatamartName()))
            .onSuccess(success -> {
                log.debug("Datamart [{}] successfully created", context.getDatamartName());
                resultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            })
            .onFailure(error -> {
                log.error("Error creating datamart [{}]!", context.getDatamartName(), error);
                resultHandler.handle(Future.failedFuture(error));
            });
    }

    private Future<Void> getDatamarAlreadyExistsFuture(DdlRequestContext context) {
        return Future.failedFuture(new DatamartAlreadyExistsException(context.getDatamartName()));
    }

    private Future<Void> createDatamartInPlugins(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) -> metadataExecutor.execute(context, promise))
            .onFailure(fail -> log.error("Error creating schema [{}] in data sources!", context.getDatamartName(), fail));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }
}
