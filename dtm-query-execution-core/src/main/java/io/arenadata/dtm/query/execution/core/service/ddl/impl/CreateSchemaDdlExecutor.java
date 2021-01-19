package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.exception.datamart.DatamartAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlRequestContext;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.core.dto.ddl.DdlType.CREATE_SCHEMA;

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
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return createDatamartIfNotExists(context);
    }

    private Future<QueryResult> createDatamartIfNotExists(DdlRequestContext context) {
        return Future.future(promise -> {
            String datamartName = ((SqlCreateDatabase) context.getSqlNode()).getName().getSimple();
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(CREATE_SCHEMA);
            context.setDatamartName(datamartName);
            datamartDao.existsDatamart(datamartName)
                    .compose(isExists -> isExists ? getDatamarAlreadyExistsFuture(datamartName) : metadataExecutor.execute(context))
                    .compose(v -> datamartDao.createDatamart(datamartName))
                    .onSuccess(success -> {
                        log.debug("Datamart [{}] successfully created", datamartName);
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> getDatamarAlreadyExistsFuture(String datamartName) {
        return Future.failedFuture(new DatamartAlreadyExistsException(datamartName));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }
}
