package io.arenadata.dtm.query.execution.plugin.adp.ddl.factory;

import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTables;
import io.arenadata.dtm.query.execution.plugin.adp.base.factory.metadata.AdpTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import lombok.val;
import org.junit.jupiter.api.Test;

import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.createAllTypesTable;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AdpCreateTableQueriesFactoryTest {
    private final TableEntitiesFactory<AdpTables<AdpTableEntity>> tableEntitiesFactory = new AdpTableEntitiesFactory();
    private final AdpCreateTableQueriesFactory tableQueriesFactory = new AdpCreateTableQueriesFactory(tableEntitiesFactory);

    @Test
    void createSuccess() {
        val expectedActual = "CREATE TABLE datamart.table_actual (id int8 NOT NULL, varchar_col varchar(10), char_col varchar(10), bigint_col int8, int_col int8, int32_col int4, double_col float8, float_col float4, date_col date, time_col time(6), timestamp_col timestamp(6), boolean_col bool, uuid_col varchar(36), link_col varchar, sys_from int8, sys_to int8, sys_op int4, constraint pk_datamart_table_actual primary key (id, sys_from))";
        val expectedHistory = "CREATE TABLE datamart.table_history (id int8 NOT NULL, varchar_col varchar(10), char_col varchar(10), bigint_col int8, int_col int8, int32_col int4, double_col float8, float_col float4, date_col date, time_col time(6), timestamp_col timestamp(6), boolean_col bool, uuid_col varchar(36), link_col varchar, sys_from int8, sys_to int8, sys_op int4, constraint pk_datamart_table_history primary key (id, sys_from))";
        val expectedStaging = "CREATE TABLE datamart.table_staging (id int8 NOT NULL, varchar_col varchar(10), char_col varchar(10), bigint_col int8, int_col int8, int32_col int4, double_col float8, float_col float4, date_col date, time_col time(6), timestamp_col timestamp(6), boolean_col bool, uuid_col varchar(36), link_col varchar, sys_from int8, sys_to int8, sys_op int4)";
        val adpTables = tableQueriesFactory.create(createAllTypesTable(), "env");
        assertEquals(expectedActual, adpTables.getActual());
        assertEquals(expectedHistory, adpTables.getHistory());
        assertEquals(expectedStaging, adpTables.getStaging());
    }
}
