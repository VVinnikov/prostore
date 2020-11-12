package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AdbCreateTableQueriesTest {
    private static final String EXPECTED_CREATE_ACTUAL_TABLE_QUERY = "CREATE TABLE test.test_ts3222_actual " +
            "(id int8 NOT NULL, name varchar , dt timestamp ," +
            " sys_from bigint, sys_to bigint, sys_op int, constraint" +
            " pk_test_test_ts3222_actual primary key (id)) DISTRIBUTED BY (id, dt)";

    private static final String EXPECTED_CREATE_HISTORY_TABLE_QUERY = "CREATE TABLE test.test_ts3222_history " +
            "(id int8 NOT NULL, name varchar , dt timestamp , " +
            "sys_from bigint, sys_to bigint, sys_op int," +
            " constraint pk_test_test_ts3222_history primary key (id, sys_from)) DISTRIBUTED BY (id, dt)";

    private static final String EXPECTED_CREATE_STAGING_TABLE_QUERY = "CREATE TABLE test.test_ts3222_staging " +
            "(id int8 NOT NULL, name varchar , dt timestamp , sys_from bigint," +
            " sys_to bigint, sys_op int, req_id varchar(36)," +
            " constraint pk_test_test_ts3222_staging primary key (id)) DISTRIBUTED BY (id, dt)";

    private AdbCreateTableQueries adbCreateTableQueries;

    @BeforeEach
    void setUp() {
        Entity entity = new Entity("test.test_ts3222", Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "name", ColumnType.VARCHAR.name(), true, null, null, null),
                new EntityField(2, "dt", ColumnType.TIMESTAMP.name(), true, null, 2, null)));
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(new QueryRequest(), entity));
        adbCreateTableQueries = new AdbCreateTableQueries(context);
    }

    @Test
    void createActualTableQueryTest() {
        assertEquals(EXPECTED_CREATE_ACTUAL_TABLE_QUERY, adbCreateTableQueries.getCreateActualTableQuery());
    }

    @Test
    void createHistoryTableQueryTest() {
        assertEquals(EXPECTED_CREATE_HISTORY_TABLE_QUERY, adbCreateTableQueries.getCreateHistoryTableQuery());
    }

    @Test
    void createStagingTableQueryTest() {
        assertEquals(EXPECTED_CREATE_STAGING_TABLE_QUERY, adbCreateTableQueries.getCreateStagingTableQuery());
    }

}
