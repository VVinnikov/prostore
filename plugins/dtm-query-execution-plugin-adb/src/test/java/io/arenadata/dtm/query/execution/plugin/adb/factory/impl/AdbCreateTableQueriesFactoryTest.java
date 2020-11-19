package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.CreateTableQueriesFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AdbCreateTableQueriesFactoryTest {
    private static final String EXPECTED_CREATE_ACTUAL_TABLE_QUERY = "CREATE TABLE test_schema.test_table_actual " +
            "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, sk_key3 int8 NOT NULL, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, DOUBLE_type float8, " +
            "FLOAT_type real, DATE_type date, TIME_type time(5), TIMESTAMP_type timestamp(5), BOOLEAN_type boolean, " +
            "UUID_type varchar(36), sys_from bigint, sys_to bigint, sys_op int, " +
            "constraint pk_test_schema_test_table_actual primary key (id, pk2)) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private static final String EXPECTED_CREATE_HISTORY_TABLE_QUERY = "CREATE TABLE test_schema.test_table_history " +
            "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, sk_key3 int8 NOT NULL, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, DOUBLE_type float8, " +
            "FLOAT_type real, DATE_type date, TIME_type time(5), TIMESTAMP_type timestamp(5), BOOLEAN_type boolean, " +
            "UUID_type varchar(36), sys_from bigint, sys_to bigint, sys_op int, " +
            "constraint pk_test_schema_test_table_history primary key (id, pk2, sys_from)) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private static final String EXPECTED_CREATE_STAGING_TABLE_QUERY = "CREATE TABLE test_schema.test_table_staging " +
            "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, sk_key3 int8 NOT NULL, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, DOUBLE_type float8, " +
            "FLOAT_type real, DATE_type date, TIME_type time(5), TIMESTAMP_type timestamp(5), BOOLEAN_type boolean, " +
            "UUID_type varchar(36), sys_from bigint, sys_to bigint, sys_op int, req_id varchar(36), " +
            "constraint pk_test_schema_test_table_staging primary key (id, pk2)) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private AdbTables<String> adbCreateTableQueries;

    @BeforeEach
    void setUp() {
        Entity entity = getEntity();
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(new QueryRequest(), entity));
        CreateTableQueriesFactory<AdbTables<String>> adbCreateTableQueriesFactory = new AdbCreateTableQueriesFactory(new AdbTableEntitiesFactory());
        adbCreateTableQueries = adbCreateTableQueriesFactory.create(context);
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

    private Entity getEntity() {
        List<EntityField> keyFields = Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "sk_key2", ColumnType.INT.name(), false, null, 2, null),
                new EntityField(2, "pk2", ColumnType.INT.name(), false, 2, null, null),
                new EntityField(3, "sk_key3", ColumnType.INT.name(), false, null, 3, null)
        );
        ColumnType[] types = ColumnType.values();
        List<EntityField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++)
        {
            ColumnType type = types[i];
            if (Arrays.asList(ColumnType.BLOB, ColumnType.ANY).contains(type)) {
                continue;
            }

            EntityField.EntityFieldBuilder builder = EntityField.builder()
                    .ordinalPosition(i + keyFields.size())
                    .type(type)
                    .nullable(true)
                    .name(type.name() + "_type");
            if (Arrays.asList(ColumnType.CHAR, ColumnType.VARCHAR).contains(type))
            {
                builder.size(20);
            }
            else if (Arrays.asList(ColumnType.TIME, ColumnType.TIMESTAMP).contains(type))
            {
                builder.accuracy(5);
            }
            fields.add(builder.build());
        }
        fields.addAll(keyFields);
        return new Entity("test_schema.test_table", fields);
    }
}
