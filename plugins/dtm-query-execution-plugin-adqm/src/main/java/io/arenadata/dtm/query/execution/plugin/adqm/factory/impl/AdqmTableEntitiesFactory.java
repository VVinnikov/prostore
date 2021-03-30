/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableColumn;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableEntity;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTables;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.DdlUtils;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants.ACTUAL_POSTFIX;
import static io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants.ACTUAL_SHARD_POSTFIX;

@Service("adqmTableEntitiesFactory")
public class AdqmTableEntitiesFactory implements TableEntitiesFactory<AdqmTables<AdqmTableEntity>> {

    private static final List<AdqmTableColumn> sysColumns = Arrays.asList(
            new AdqmTableColumn("sys_from", "Int64", false),
            new AdqmTableColumn("sys_to", "Int64", false),
            new AdqmTableColumn("sys_op", "Int8", false),
            new AdqmTableColumn(Constants.SYS_CLOSE_DATE_FIELD, "DateTime", false),
            new AdqmTableColumn("sign", "Int8", false)
    );

    @Override
    public AdqmTables<AdqmTableEntity> create(Entity entity, String env) {
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
        // TODO Should we fail if sharding column in metatable of unsupported type?
        // CH support only not null int types as sharding key
        return fields.stream()
                .filter(f -> f.getShardingOrder() != null)
                .sorted(Comparator.comparing(EntityField::getShardingOrder))
                .map(EntityField::getName)
                .collect(Collectors.toList());
    }
}
