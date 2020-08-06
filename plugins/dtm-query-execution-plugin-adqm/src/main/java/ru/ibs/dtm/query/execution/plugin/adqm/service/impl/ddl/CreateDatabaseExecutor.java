package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

@Component
@Slf4j
public class CreateDatabaseExecutor implements DdlExecutor<Void> {
    private static final String CREATE_TEMPLATE = "CREATE DATABASE %s__%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final DropDatabaseExecutor dropDatabaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;

    public CreateDatabaseExecutor(DatabaseExecutor databaseExecutor,
                                  DdlProperties ddlProperties,
                                  AppConfiguration appConfiguration, DropDatabaseExecutor dropDatabaseExecutor) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
        this.dropDatabaseExecutor = dropDatabaseExecutor;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        SqlNode query = context.getQuery();
        if (!(query instanceof SqlCreateDatabase)) {
            handler.handle(Future.failedFuture(String.format("Expecting SqlCreateDatabase in context, receiving: %s", context)));
            return;
        }

        String name = ((SqlCreateDatabase) query).getName().names.get(0);

        DdlRequestContext dropCtx = createDropRequestContext(name);
        dropDatabaseExecutor.execute(dropCtx, SqlKind.DROP_SCHEMA.lowerName, ar -> {
            if (ar.succeeded()) {
                createDatabase(name).onComplete(handler);
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private DdlRequestContext createDropRequestContext(String schemaName) {
        DdlRequestContext dropCtx = new DdlRequestContext(new DdlRequest(new QueryRequest()));
        dropCtx.setDatamartName(schemaName);
        return dropCtx;
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> createDatabase(String dbname) {
        String cluster = ddlProperties.getCluster();
        String createCmd = String.format(CREATE_TEMPLATE, appConfiguration.getSystemName(), dbname, cluster);
        return databaseExecutor.executeUpdate(createCmd);
    }
}
