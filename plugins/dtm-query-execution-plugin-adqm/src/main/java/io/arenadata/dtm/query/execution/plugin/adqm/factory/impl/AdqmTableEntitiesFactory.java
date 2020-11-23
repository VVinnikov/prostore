package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adqm.common.Constants;
import io.arenadata.dtm.query.execution.plugin.adqm.common.DdlUtils;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableColumn;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableEntity;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTables;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TableEntitiesFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adqm.common.Constants.ACTUAL_SHARD_POSTFIX;

@Service("adqmTableEntitiesFactory")
public class AdqmTableEntitiesFactory implements TableEntitiesFactory<AdqmTables<AdqmTableEntity>> {

    private static final List<AdqmTableColumn> sysColumns = Arrays.asList(
            new AdqmTableColumn("sys_from", "Int64", false),
            new AdqmTableColumn("sys_to", "Int64", false),
            new AdqmTableColumn("sys_op", "Int8", false),
            new AdqmTableColumn("close_date", "DateTime", false),
            new AdqmTableColumn("sign", "Int8", false)
    );

    @Override
    public AdqmTables<AdqmTableEntity> create(DdlRequestContext context) {
        Entity entity = context.getRequest().getEntity();
        String env = context.getRequest().getQueryRequest().getEnvName();
        String tableName = entity.getName();
        String schema = entity.getSchema();
        List<EntityField> fields = entity.getFields();
        List<AdqmTableColumn> columns = fields.stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(this::transformColumn)
                .collect(Collectors.toList());
        columns.addAll(sysColumns);
        List<String> sortedKeys = getSortedKeys(fields);
        List<String> shardingKeys = getShardingKeys(fields);
        return new AdqmTables<>(
                getBaseEntityBuilder(env, tableName + ACTUAL_SHARD_POSTFIX, schema, columns)
                        .sortedKeys(sortedKeys)
                        .build(),
                getBaseEntityBuilder(env, tableName + ACTUAL_POSTFIX, schema, columns)
                        .shardingKeys(shardingKeys)
                        .build());
    }

    private AdqmTableEntity.AdqmTableEntityBuilder getBaseEntityBuilder(String env,
                                                                        String tableName,
                                                                        String schema,
                                                                        List<AdqmTableColumn> columns) {
        return AdqmTableEntity.builder()
                .env(env)
                .name(tableName)
                .schema(schema)
                .columns(columns);
    }

    private AdqmTableColumn transformColumn(EntityField field) {
        return new AdqmTableColumn(field.getName(), DdlUtils.classTypeToNative(field.getType()), field.getNullable());
    }

    private List<String> getSortedKeys(List<EntityField> fields) {
        List<String> orderKeys = fields.stream().filter(f -> f.getPrimaryOrder() != null)
                .map(EntityField::getName).collect(Collectors.toList());
        orderKeys.add(Constants.SYS_FROM_FIELD);
        return orderKeys;
    }

    private List<String> getShardingKeys(List<EntityField> fields) {
        // TODO Check against CH, does it support several columns as distributed key?
        // TODO Should we fail if sharding column in metatable of unsupported type?
        // CH support only not null int types as sharding key
        return fields.stream().filter(f -> f.getShardingOrder() != null)
                .map(EntityField::getName).limit(1).collect(Collectors.toList());
    }
}
