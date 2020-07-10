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
import ru.ibs.dtm.query.calcite.core.extension.eddl.DropDatabase;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

@Component
@Slf4j
public class DropDatabaseExecutor implements DdlExecutor<Void> {
    private static final String DROP_TEMPLATE = "DROP DATABASE IF EXISTS %s__%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;

    public DropDatabaseExecutor(DatabaseExecutor databaseExecutor,
                                DdlProperties ddlProperties,
                                AppConfiguration appConfiguration) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        SqlNode query = context.getQuery();
        if (!(query instanceof DropDatabase)) {
            handler.handle(Future.failedFuture(String.format("Expecting DropDatabase in context, receiving: %s", context)));
            return;
        }

        String name = ((DropDatabase) query).getName().names.get(0);

        dropDatabase(name).onComplete(handler);
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }

    @Override
    @Autowired
    public void registration(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> dropDatabase(String dbname) {
        String cluster = ddlProperties.getCluster();
        String dropCmd = String.format(DROP_TEMPLATE, appConfiguration.getSystemName(), dbname, cluster);
        return databaseExecutor.executeUpdate(dropCmd);
    }
}
