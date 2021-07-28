package io.arenadata.dtm.query.execution.plugin.adb.ddl.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adb.base.dto.metadata.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.base.factory.metadata.AdbTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.impl.AdbCreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import utils.CreateEntityUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AdbCreateTableQueriesFactoryTest {
    private static final String EXPECTED_CREATE_ACTUAL_TABLE_QUERY = "CREATE TABLE test_schema.test_table_actual " +
            "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, sk_key3 int8 NOT NULL, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, " +
            "FLOAT_type float4, DATE_type date, TIME_type time(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, " +
            "UUID_type varchar(36), LINK_type varchar, sys_from int8, sys_to int8, sys_op int4, " +
            "constraint pk_test_schema_test_table_actual primary key (id, pk2, sys_from)) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private static final String EXPECTED_CREATE_HISTORY_TABLE_QUERY = "CREATE TABLE test_schema.test_table_history " +
            "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, sk_key3 int8 NOT NULL, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, " +
            "FLOAT_type float4, DATE_type date, TIME_type time(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, " +
            "UUID_type varchar(36), LINK_type varchar, sys_from int8, sys_to int8, sys_op int4, " +
            "constraint pk_test_schema_test_table_history primary key (id, pk2, sys_from)) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private static final String EXPECTED_CREATE_STAGING_TABLE_QUERY = "CREATE TABLE test_schema.test_table_staging " +
            "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, sk_key3 int8 NOT NULL, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, " +
            "FLOAT_type float4, DATE_type date, TIME_type time(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, " +
            "UUID_type varchar(36), LINK_type varchar, sys_from int8, sys_to int8, sys_op int4) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private static AdbTables<String> adbCreateTableQueries;

    @BeforeAll
    static void setUp() {
        Entity entity = CreateEntityUtils.getEntity();
        CreateTableQueriesFactory<AdbTables<String>> adbCreateTableQueriesFactory = new AdbCreateTableQueriesFactory(new AdbTableEntitiesFactory());
        adbCreateTableQueries = adbCreateTableQueriesFactory.create(entity, "");
    }

    @Test
    void createActualTableQueryTest() {
        assertEquals(EXPECTED_CREATE_ACTUAL_TABLE_QUERY, adbCreateTableQueries.getActual());
    }

    @Test
    void createHistoryTableQueryTest() {
        assertEquals(EXPECTED_CREATE_HISTORY_TABLE_QUERY, adbCreateTableQueries.getHistory());
    }

    @Test
    void createStagingTableQueryTest() {
        assertEquals(EXPECTED_CREATE_STAGING_TABLE_QUERY, adbCreateTableQueries.getStaging());
    }
}
