package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableDdlTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());

    @BeforeAll
    public static void setup() {
        ddlProperties.setTtlSec(3600);
        ddlProperties.setCluster("test_arenadata");
        ddlProperties.setArchiveDisk("default");
    }

    @Test
    public void testCreateTable() {
        MockDatabaseExecutor mockExecutor = new MockDatabaseExecutor(
                Arrays.asList(
                        s -> s.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.test_actual ON CLUSTER test_arenadata"),
                        s -> s.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.test_actual_shard ON CLUSTER test_arenadata"),
                        s -> s.contains("CREATE TABLE dev__shares.test_actual_shard ON CLUSTER test_arenadata") &&
                                s.contains("ORDER BY (test2, test3, sys_from)") &&
                                s.contains("test1 Nullable(String), test2 Int64, test3 Int64"),
                        s -> s.contains("CREATE TABLE dev__shares.test_actual ON CLUSTER test_arenadata") &&
                                s.contains("Engine = Distributed(test_arenadata, dev__shares, test_actual_shard, test4)")
                ));
        DropTableExecutor dropTableExecutor = new DropTableExecutor(mockExecutor, ddlProperties, appConfiguration);
        CreateTableExecutor executor = new CreateTableExecutor(mockExecutor, ddlProperties, appConfiguration, dropTableExecutor);

        Entity tbl = new Entity("shares.test",
                Arrays.asList(
                        new EntityField(0,"test1", "VARCHAR(255)", true, null, null, ""),
                        new EntityField(1,"test2", "INT", false, 1, null, ""),
                        new EntityField(2,"test3", "INT", false, 2, null, ""),
                        new EntityField(3,"test4", "VARCHAR(255)", true, null, 1, ""),
                        new EntityField(4,"test5", "VARCHAR(255)", true, null, 2, "")
                ));

        DdlRequestContext context = new DdlRequestContext(new DdlRequest(new QueryRequest(), tbl));

        executor.execute(context, "CREATE", ar -> {
            assertTrue(ar.succeeded());
            assertEquals(mockExecutor.getExpectedCalls().size(), mockExecutor.getCallCount(), "All calls should be performed");
        });
    }

    @Test
    public void testDropTable() {
        MockDatabaseExecutor mockExecutor = new MockDatabaseExecutor(
                Arrays.asList(
                        s -> s.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.test_actual ON CLUSTER test_arenadata"),
                        s -> s.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.test_actual_shard ON CLUSTER test_arenadata")
                ));
        DdlExecutor<Void> executor = new DropTableExecutor(mockExecutor, ddlProperties, appConfiguration);

        Entity tbl = new Entity("shares.test", Collections.emptyList());

        DdlRequestContext context = new DdlRequestContext(new DdlRequest(new QueryRequest(), tbl));

        executor.execute(context, "DROP", ar -> {
            assertTrue(ar.succeeded());
            assertEquals(mockExecutor.getExpectedCalls().size(), mockExecutor.getCallCount(), "All calls should be performed");
        });
    }
}
