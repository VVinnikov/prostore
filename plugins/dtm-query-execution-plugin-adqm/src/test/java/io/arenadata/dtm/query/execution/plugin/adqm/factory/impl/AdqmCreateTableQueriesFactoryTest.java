package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl.AdqmCreateTableQueries;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.CreateTableQueriesFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class AdqmCreateTableQueriesFactoryTest {

    private static final String EXPECTED_CREATE_SHARD_TABLE_QUERY = "CREATE TABLE dev__test.test_ts3222_actual_shard " +
            "ON CLUSTER test_arenadata\n" +
            "(\n" +
            "  id Int64, name Nullable(String), dt Nullable(DateTime64),\n" +
            "  sys_from   Int64,\n" +
            "  sys_to     Int64,\n" +
            "  sys_op     Int8,\n" +
            "  close_date DateTime,\n" +
            "  sign       Int8\n" +
            ")\n" +
            "ENGINE = CollapsingMergeTree(sign)\n" +
            "ORDER BY (id, sys_from)\n" +
            "TTL close_date + INTERVAL 3600 SECOND TO DISK 'default'";

    private static final String EXPECTED_CREATE_DISTRIBUTED_TABLE_QUERY = "CREATE TABLE dev__test.test_ts3222_actual " +
            "ON CLUSTER test_arenadata\n" +
            "(\n" +
            "  id Int64, name Nullable(String), dt Nullable(DateTime64),\n" +
            "  sys_from   Int64,\n" +
            "  sys_to     Int64,\n" +
            "  sys_op     Int8,\n" +
            "  close_date DateTime,\n" +
            "  sign       Int8\n" +
            ")\n" +
            "Engine = Distributed(test_arenadata, dev__test, test_ts3222_actual_shard, id)";

    private AdqmCreateTableQueries adqmCreateTableQueries;

    @BeforeEach
    void setUp() {
        Entity entity = new Entity("test.test_ts3222", Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "name", ColumnType.VARCHAR.name(), true, null, null, null),
                new EntityField(2, "dt", ColumnType.TIMESTAMP.name(), true, null, 2, null)));
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(new QueryRequest(), entity));

        DdlProperties ddlProperties = new DdlProperties();
        ddlProperties.setTtlSec(3600);
        ddlProperties.setCluster("test_arenadata");
        ddlProperties.setArchiveDisk("default");
        AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());
        CreateTableQueriesFactory<AdqmCreateTableQueries> adbCreateTableQueriesFactory =
                new AdqmCreateTableQueriesFactory(ddlProperties, appConfiguration);
        adqmCreateTableQueries = adbCreateTableQueriesFactory.create(context);
    }

    @Test
    void createShardTableQueryTest() {
        assertEquals(EXPECTED_CREATE_SHARD_TABLE_QUERY, adqmCreateTableQueries.getShard());
    }

    @Test
    void createDistributedTableQueryTest() {
        assertEquals(EXPECTED_CREATE_DISTRIBUTED_TABLE_QUERY, adqmCreateTableQueries.getDistributed());
    }
}
