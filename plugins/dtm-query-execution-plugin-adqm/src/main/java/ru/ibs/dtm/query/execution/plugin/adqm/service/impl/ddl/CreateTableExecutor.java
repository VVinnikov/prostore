package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.common.Constants;
import ru.ibs.dtm.query.execution.plugin.adqm.common.DdlUtils;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.util.List;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_POSTFIX;
import static ru.ibs.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_SHARD_POSTFIX;

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {
    private final static String CREATE_SHARD_TABLE_TEMPLATE =
            "CREATE TABLE %s__%s.%s ON CLUSTER %s\n" +
                    "(\n" +
                    "  %s,\n" +
                    "  sys_from   Int64,\n" +
                    "  sys_to     Int64,\n" +
                    "  sys_op     Int8,\n" +
                    "  close_date DateTime,\n" +
                    "  sign       Int8\n" +
                    ")\n" +
                    "ENGINE = CollapsingMergeTree(sign)\n" +
                    "ORDER BY (%s)\n" +
                    "TTL close_date + INTERVAL %d SECOND TO DISK '%s'";

    private final static String CREATE_DISTRIBUTED_TABLE_TEMPLATE =
            "CREATE TABLE %s__%s.%s ON CLUSTER %s\n" +
                    "(\n" +
                    "  %s,\n" +
                    "  sys_from   Int64,\n" +
                    "  sys_to     Int64,\n" +
                    "  sys_op     Int8,\n" +
                    "  close_date DateTime,\n" +
                    "  sign       Int8\n" +
                    ")\n" +
                    "Engine = Distributed(%s, %s__%s, %s, %s)";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;
    private final AppConfiguration appConfiguration;
    private final DropTableExecutor dropTableExecutor;

    public CreateTableExecutor(DatabaseExecutor databaseExecutor,
                               DdlProperties ddlProperties,
                               AppConfiguration appConfiguration, DropTableExecutor dropTableExecutor) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
        this.dropTableExecutor = dropTableExecutor;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        Entity tbl = context.getRequest().getClassTable();
        DdlRequestContext dropCtx = new DdlRequestContext(new DdlRequest(new QueryRequest(), tbl));

        dropTableExecutor.execute(dropCtx, SqlKind.DROP_TABLE.lowerName, ar -> createTable(tbl).onComplete(handler));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> createTable(Entity entity) {
        String env = appConfiguration.getSystemName();
        String cluster = ddlProperties.getCluster();
        String schema = entity.getSchema();
        String table = entity.getName();
        String columnList = getColumns(entity.getFields());
        String orderList = getOrderKeys(entity.getFields());
        String shardingList = getShardingKeys(entity.getFields());
        Integer ttlSec = ddlProperties.getTtlSec();
        String archiveDisk = ddlProperties.getArchiveDisk();

        String createShard = String.format(CREATE_SHARD_TABLE_TEMPLATE,
                env, schema, table + ACTUAL_SHARD_POSTFIX, cluster, columnList, orderList, ttlSec, archiveDisk);

        String createDistributed = String.format(CREATE_DISTRIBUTED_TABLE_TEMPLATE,
                env, schema, table + ACTUAL_POSTFIX, cluster, columnList, cluster, env, schema,
                table + ACTUAL_SHARD_POSTFIX, shardingList);

        return databaseExecutor.executeUpdate(createShard)
                .compose(v ->
                        databaseExecutor.executeUpdate(createDistributed));
    }

    private String getColumns(List<EntityField> fields) {
        return fields.stream().map(DdlUtils::classFieldToString).collect(Collectors.joining(", "));
    }

    private String getOrderKeys(List<EntityField> fields) {
        List<String> orderKeys = fields.stream().filter(f -> f.getPrimaryOrder() != null)
                .map(EntityField::getName).collect(Collectors.toList());
        orderKeys.add(Constants.SYS_FROM_FIELD);
        return String.join(", ", orderKeys);
    }

    private String getShardingKeys(List<EntityField> fields) {
        // TODO Check against CH, does it support several columns as distributed key?
        // TODO Should we fail if sharding column in metatable of unsupported type?
        // CH support only not null int types as sharding key
        return fields.stream().filter(f -> f.getShardingOrder() != null)
                .map(EntityField::getName).limit(1).collect(Collectors.joining(", "));
    }
}
