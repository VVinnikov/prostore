package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypes;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetadataSqlFactoryImplTest {
    private static final String EXPECTED_DROP_SCRIPTS = "DROP TABLE IF EXISTS test.test_ts3222_actual;" +
            " DROP TABLE IF EXISTS test.test_ts3222_history; " +
            "DROP TABLE IF EXISTS test.test_ts3222_staging; ";
    private static final String EXPECTED_CREATE_SCRIPTS = "CREATE TABLE test.test_ts3222_actual " +
            "(id integer NOT NULL, name varchar , dt timestamp ," +
            " sys_from bigint, sys_to bigint, sys_op int, constraint" +
            " pk_test_test_ts3222_actual primary key (id)) DISTRIBUTED BY (id, dt);" +
            " CREATE TABLE test.test_ts3222_history " +
            "(id integer NOT NULL, name varchar , dt timestamp , " +
            "sys_from bigint, sys_to bigint, sys_op int," +
            " constraint pk_test_test_ts3222_history primary key (id)) DISTRIBUTED BY (id, dt); " +
            "CREATE TABLE test.test_ts3222_staging " +
            "(id integer NOT NULL, name varchar , dt timestamp , sys_from bigint," +
            " sys_to bigint, sys_op int, req_id varchar(36)," +
            " constraint pk_test_test_ts3222_staging primary key (id)) DISTRIBUTED BY (id, dt); ";

    @Test
    void createDropTableScript() {
        String tableScript = new MetadataSqlFactoryImpl().createDropTableScript(getClassTable());
        assertEquals(EXPECTED_DROP_SCRIPTS, tableScript);
    }

    @Test
    void createTableScripts() {
        String tableScript = new MetadataSqlFactoryImpl().createTableScripts(getClassTable());
        assertEquals(EXPECTED_CREATE_SCRIPTS, tableScript);
    }

    private ClassTable getClassTable() {
        return new ClassTable("test.test_ts3222", Arrays.asList(
                new ClassField("id", ClassTypes.INT.name(), false, 1, 1, null),
                new ClassField("name", ClassTypes.VARCHAR.name(), true, null, null, null),
                new ClassField("dt", ClassTypes.DATETIME.name(), true, null, 2, null)
        ));
    }
}
