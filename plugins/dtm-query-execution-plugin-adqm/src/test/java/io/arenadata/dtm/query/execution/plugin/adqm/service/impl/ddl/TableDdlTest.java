package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

//FixMe Test
class TableDdlTest {
//    private static final DdlProperties ddlProperties = new DdlProperties();
//    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());
//
//    @BeforeAll
//    public static void setup() {
//        ddlProperties.setTtlSec(3600);
//        ddlProperties.setCluster("test_arenadata");
//        ddlProperties.setArchiveDisk("default");
//    }
//
//    @Test
//    public void testDropTable() {
//        MockDatabaseExecutor mockExecutor = new MockDatabaseExecutor(
//                Arrays.asList(
//                        s -> s.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.test_actual ON CLUSTER test_arenadata"),
//                        s -> s.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.test_actual_shard ON CLUSTER test_arenadata")
//                ));
//        DdlExecutor<Void> executor = new DropTableExecutor(mockExecutor, ddlProperties, appConfiguration);
//
//        Entity tbl = new Entity("shares.test", Collections.emptyList());
//
//        DdlRequestContext context = new DdlRequestContext(new DdlRequest(new QueryRequest(), tbl));
//
//        executor.execute(context, "DROP")
//                .onComplete(ar -> {
//                    assertTrue(ar.succeeded());
//                    assertEquals(mockExecutor.getExpectedCalls().size(),
//                            mockExecutor.getCallCount(),
//                            "All calls should be performed");
//                });
//    }
}
