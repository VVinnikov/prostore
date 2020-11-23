package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTableEntity;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmTables;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.CreateTableQueriesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.common.DdlUtils.NOT_NULLABLE_FIELD;
import static io.arenadata.dtm.query.execution.plugin.adqm.common.DdlUtils.NULLABLE_FIELD;

@Service("adqmCreateTableQueriesFactory")
public class AdqmCreateTableQueriesFactory implements CreateTableQueriesFactory<AdqmTables<String>> {

    public final static String CREATE_SHARD_TABLE_TEMPLATE =
            "CREATE TABLE %s__%s.%s ON CLUSTER %s\n" +
                    "(%s)\n" +
                    "ENGINE = CollapsingMergeTree(sign)\n" +
                    "ORDER BY (%s)\n" +
                    "TTL close_date + INTERVAL %d SECOND TO DISK '%s'";

    public final static String CREATE_DISTRIBUTED_TABLE_TEMPLATE =
            "CREATE TABLE %s__%s.%s ON CLUSTER %s\n" +
                    "(%s)\n" +
                    "Engine = Distributed(%s, %s__%s, %s, %s)";

    private final DdlProperties ddlProperties;
    private final AdqmTableEntitiesFactory adqmTableEntitiesFactory;

    @Autowired
    public AdqmCreateTableQueriesFactory(DdlProperties ddlProperties,
                                         AdqmTableEntitiesFactory adqmTableEntitiesFactory) {
        this.ddlProperties = ddlProperties;
        this.adqmTableEntitiesFactory = adqmTableEntitiesFactory;
    }

    @Override
    public AdqmTables<String> create(DdlRequestContext context) {
        String cluster = ddlProperties.getCluster();
        Integer ttlSec = ddlProperties.getTtlSec();
        String archiveDisk = ddlProperties.getArchiveDisk();

        AdqmTables<AdqmTableEntity> tables = adqmTableEntitiesFactory.create(context);
        AdqmTableEntity shard = tables.getShard();
        AdqmTableEntity distributed = tables.getDistributed();
        String columns = distributed.getColumns().stream()
                .map(col -> String.format(col.getNullable() ? NULLABLE_FIELD : NOT_NULLABLE_FIELD,
                        col.getName(), col.getType()))
                .collect(Collectors.joining(", "));
        return new AdqmTables<>(
                String.format(CREATE_SHARD_TABLE_TEMPLATE,
                        shard.getEnv(), shard.getSchema(), shard.getName(), cluster, columns,
                        String.join(", ", shard.getSortedKeys()), ttlSec, archiveDisk),
                String.format(CREATE_DISTRIBUTED_TABLE_TEMPLATE,
                        distributed.getEnv(), distributed.getSchema(), distributed.getName(), cluster,
                        columns, cluster, distributed.getEnv(), distributed.getSchema(),
                        shard.getName(), String.join(", ", distributed.getShardingKeys()))
        );
    }
}
