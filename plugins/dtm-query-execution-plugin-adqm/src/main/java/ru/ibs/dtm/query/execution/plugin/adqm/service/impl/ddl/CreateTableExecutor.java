package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypes;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {
    private final static String ACTUAL_TABLE = "_actual";
    private final static String SHARD_TABLE = "_actual_shard";

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

    private final static String NULLABLE_FIELD = "%s Nullable(%s)";
    private final static String NOT_NULLABLE_FIELD = "%s %s";

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
        ClassTable tbl = context.getRequest().getClassTable();
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

    private Future<Void> createTable(ClassTable classTable) {
        String env = appConfiguration.getSystemName();
        String cluster = ddlProperties.getCluster();
        String schema = classTable.getSchema();
        String table = classTable.getName();
        String columnList = getColumns(classTable.getFields());
        String orderList = getOrderKeys(classTable.getFields());
        String shardingList = getShardingKeys(classTable.getFields());
        Integer ttlSec = ddlProperties.getTtlSec();
        String archiveDisk = ddlProperties.getArchiveDisk();

        String createShard = String.format(CREATE_SHARD_TABLE_TEMPLATE,
                env, schema, table + SHARD_TABLE, cluster, columnList, orderList, ttlSec, archiveDisk);

        String createDistributed = String.format(CREATE_DISTRIBUTED_TABLE_TEMPLATE,
                env, schema, table + ACTUAL_TABLE, cluster, columnList, cluster, env, schema,
                table + SHARD_TABLE, shardingList);

        return databaseExecutor.executeUpdate(createShard)
                .compose(v ->
                        databaseExecutor.executeUpdate(createDistributed));
    }

    private String getColumns(List<ClassField> fields) {
        return fields.stream().map(this::classFieldToString).collect(Collectors.joining(", "));
    }

    private String getOrderKeys(List<ClassField> fields) {
        List<String> orderKeys = fields.stream().filter(f -> f.getPrimaryOrder() != null)
                .map(ClassField::getName).collect(Collectors.toList());
        orderKeys.add("sys_from");
        return String.join(", ", orderKeys);
    }

    private String getShardingKeys(List<ClassField> fields) {
        // TODO Check against CH, does it support several columns as distributed key?
        // TODO Should we fail if sharding column in metatable of unsupported type?
        // CH support only not null int types as sharding key
        return fields.stream().filter(f -> f.getShardingOrder() != null)
                .map(ClassField::getName).limit(1).collect(Collectors.joining(", "));
    }

    private String classFieldToString(ClassField f) {
        String name = f.getName();
        String type = mapType(f.getType());
        String template = f.getNullable() ? NULLABLE_FIELD : NOT_NULLABLE_FIELD;

        return String.format(template, name, type);
    }

    private String mapType(ClassTypes type) {
        switch (type) {
            case UUID: return "UUID";

            case ANY:
            case CHAR:
            case VARCHAR: return "String";

            case INT:
            case INTEGER:
            case BIGINT:
            case DATE:
            case TIME: return "Int64";

            case TINYINT: return "Int8";

            case BOOL:
            case BOOLEAN: return "UInt8";

            case FLOAT: return "Float32";
            case DOUBLE: return "Float64";

            case DATETIME: return "DateTime";
            case TIMESTAMP: return "DateTime64(3)";

            // FIXME will be supported after ClassFiled will support them
//            case DECIMAL: return "";
//            case DEC: return "";
//            case NUMERIC: return "";
//            case FIXED: return "";
        }
        return "";
    }

}
