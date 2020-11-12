package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adqm.common.Constants;
import io.arenadata.dtm.query.execution.plugin.adqm.common.DdlUtils;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_SHARD_POSTFIX;

public class AdqmCreateTableQueries {
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

    private final String CREATE_SHARD_TABLE_QUERY;
    private final String CREATE_DISTRIBUTED_TABLE_QUERY;


    public AdqmCreateTableQueries(DdlRequestContext context,
                                  DdlProperties ddlProperties,
                                  AppConfiguration appConfiguration) {
        Entity entity = context.getRequest().getEntity();
        String env = appConfiguration.getSystemName();
        String cluster = ddlProperties.getCluster();
        String schema = entity.getSchema();
        String table = entity.getName();
        String columnList = getColumns(entity.getFields());
        String orderList = getOrderKeys(entity.getFields());
        String shardingList = getShardingKeys(entity.getFields());
        Integer ttlSec = ddlProperties.getTtlSec();
        String archiveDisk = ddlProperties.getArchiveDisk();
        CREATE_SHARD_TABLE_QUERY = String.format(CREATE_SHARD_TABLE_TEMPLATE,
                env, schema, table + ACTUAL_SHARD_POSTFIX, cluster, columnList, orderList, ttlSec, archiveDisk);
        CREATE_DISTRIBUTED_TABLE_QUERY = String.format(CREATE_DISTRIBUTED_TABLE_TEMPLATE,
                env, schema, table + ACTUAL_POSTFIX, cluster, columnList, cluster, env, schema,
                table + ACTUAL_SHARD_POSTFIX, shardingList);
    }

    public String getCreateShardTableQuery() {
        return CREATE_SHARD_TABLE_QUERY;
    }

    public String getCreateDistributedTableQuery() {
        return CREATE_DISTRIBUTED_TABLE_QUERY;
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
