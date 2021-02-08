package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableColumn;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableEntity;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTables;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.utils.DdlUtils.NOT_NULLABLE_FIELD;
import static io.arenadata.dtm.query.execution.plugin.adqm.utils.DdlUtils.NULLABLE_FIELD;

@Service("adqmCreateTableQueriesFactory")
public class AdqmCreateTableQueriesFactory implements CreateTableQueriesFactory<AdqmTables<String>> {

    private static final String CREATE_SHARD_TABLE_TEMPLATE =
            "CREATE TABLE %s__%s.%s ON CLUSTER %s\n" +
                    "(%s)\n" +
                    "ENGINE = CollapsingMergeTree(sign)\n" +
                    "ORDER BY (%s)\n" +
                    "TTL sys_close_date + INTERVAL %d SECOND TO DISK '%s'";

    private static final String CREATE_DISTRIBUTED_TABLE_TEMPLATE =
            "CREATE TABLE %s__%s.%s ON CLUSTER %s\n" +
                    "(%s)\n" +
                    "Engine = Distributed(%s, %s__%s, %s, %s)";

    private final DdlProperties ddlProperties;
    private final TableEntitiesFactory<AdqmTables<AdqmTableEntity>> tableEntitiesFactory;

    @Autowired
    public AdqmCreateTableQueriesFactory(DdlProperties ddlProperties,
                                         TableEntitiesFactory<AdqmTables<AdqmTableEntity>> tableEntitiesFactory) {
        this.ddlProperties = ddlProperties;
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public AdqmTables<String> create(Entity entity, String envName) {
        String cluster = ddlProperties.getCluster();
        Integer ttlSec = ddlProperties.getTtlSec();
        String archiveDisk = ddlProperties.getArchiveDisk();

        AdqmTables<AdqmTableEntity> tables = tableEntitiesFactory.create(entity, envName);
        AdqmTableEntity shard = tables.getShard();
        AdqmTableEntity distributed = tables.getDistributed();
        return new AdqmTables<>(
                String.format(CREATE_SHARD_TABLE_TEMPLATE,
                        shard.getEnv(),
                        shard.getSchema(),
                        shard.getName(),
                        cluster,
                        getColumnsQuery(shard.getColumns()),
                        String.join(", ", shard.getSortedKeys()),
                        ttlSec,
                        archiveDisk),
                String.format(CREATE_DISTRIBUTED_TABLE_TEMPLATE,
                        distributed.getEnv(),
                        distributed.getSchema(),
                        distributed.getName(),
                        cluster,
                        getColumnsQuery(distributed.getColumns()),
                        cluster,
                        distributed.getEnv(),
                        distributed.getSchema(),
                        shard.getName(),
                        String.join("+", distributed.getShardingKeys()))
        );
    }

    private String getColumnsQuery(List<AdqmTableColumn> columns) {
        return columns.stream()
                .map(col -> String.format(col.getNullable() ? NULLABLE_FIELD : NOT_NULLABLE_FIELD,
                        col.getName(), col.getType()))
                .collect(Collectors.joining(", "));
    }
}
