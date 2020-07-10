package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {
    private final static String ACTUAL_TABLE = "_actual";
    private final static String SHARD_TABLE = "_actual_shard";

    private final static String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS %s__%s.%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;

    public DropTableExecutor(DatabaseExecutor databaseExecutor,
                             DdlProperties ddlProperties,
                             AppConfiguration appConfiguration) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
    }


    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        dropTable(context.getRequest().getClassTable()).onComplete(handler);
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    private Future<Void> dropTable(ClassTable classTable) {
        String env = appConfiguration.getSystemName();

        String cluster = ddlProperties.getCluster();
        String schema = classTable.getSchema();
        String table = classTable.getName();

        String dropShard = String.format(DROP_TABLE_TEMPLATE, env, schema, table + SHARD_TABLE, cluster);
        String dropDistributed = String.format(DROP_TABLE_TEMPLATE, env, schema, table + ACTUAL_TABLE, cluster);

        return databaseExecutor.executeUpdate(dropDistributed)
                .compose(v ->
                        databaseExecutor.executeUpdate(dropShard));
    }

}
