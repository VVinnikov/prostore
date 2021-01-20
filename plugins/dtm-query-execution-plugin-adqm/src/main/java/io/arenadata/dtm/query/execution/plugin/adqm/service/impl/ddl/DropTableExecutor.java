package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants.ACTUAL_SHARD_POSTFIX;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {
    private final static String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS %s__%s.%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;

    @Autowired
    public DropTableExecutor(DatabaseExecutor databaseExecutor,
                             DdlProperties ddlProperties) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return dropTable(request.getEnvName(), request.getEntity());
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> dropTable(String envName, Entity entity) {

        String cluster = ddlProperties.getCluster();
        String schema = entity.getSchema();
        String table = entity.getName();

        String dropDistributed = String.format(DROP_TABLE_TEMPLATE, envName, schema, table + ACTUAL_POSTFIX, cluster);
        String dropShard = String.format(DROP_TABLE_TEMPLATE, envName, schema, table + ACTUAL_SHARD_POSTFIX, cluster);

        return databaseExecutor.executeUpdate(dropDistributed)
                .compose(v -> databaseExecutor.executeUpdate(dropShard));
    }

}
