package ru.ibs.dtm.query.execution.plugin.adqm.factory.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataFactoryImplTest {

    private static class MockDatabaseExecutor implements DatabaseExecutor {
        final List<Predicate<String>> expectedCalls;
        int callCount;

        private MockDatabaseExecutor(List<Predicate<String>> expectedCalls) {
            this.expectedCalls = expectedCalls;
        }

        @Override
        public void execute(String sql, Handler<AsyncResult<JsonArray>> resultHandler) {

        }

        @Override
        public void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler) {
            callCount++;
            if (expectedCalls.size() < callCount) {
                completionHandler.handle(
                        Future.failedFuture(String.format("Invalid call count, expected %d, got %d", expectedCalls.size(), callCount)));
                return;
            }

            Predicate<String> expected = expectedCalls.get(callCount - 1);
            if (expected.test(sql)) {
                completionHandler.handle(Future.succeededFuture());
                return;
            }

            completionHandler.handle(Future.failedFuture(String.format("Cannot test SQL, got %s",  sql)));
        }

        @Override
        public void executeWithParams(String sql, List<Object> params, Handler<AsyncResult<?>> resultHandler) {

        }
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
        MetadataFactory metadataFactory = new MetadataFactoryImpl(mockExecutor, getDdlProperties());

        ClassTable tbl = new ClassTable("shares.test",
                Arrays.asList(
                        new ClassField("test1", "VARCHAR(255)", true, null, null, ""),
                        new ClassField("test2", "INTEGER", false, 1, null, ""),
                        new ClassField("test3", "INTEGER", false, 2, null, ""),
                        new ClassField("test4", "VARCHAR(255)", true, null, 1, ""),
                        new ClassField("test5", "VARCHAR(255)", true, null, 2, "")
                ));

        metadataFactory.apply(tbl, ar -> {
            assertTrue(ar.succeeded());
            assertEquals(mockExecutor.expectedCalls.size(), mockExecutor.callCount, "All calls should be performed");
        });
    }

    @Test
    public void testDropTable() {
        MockDatabaseExecutor mockExecutor = new MockDatabaseExecutor(
                Arrays.asList(
                        s -> s.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.test_actual ON CLUSTER test_arenadata"),
                        s -> s.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.test_actual_shard ON CLUSTER test_arenadata")
                ));
        MetadataFactory metadataFactory = new MetadataFactoryImpl(mockExecutor, getDdlProperties());

        ClassTable tbl = new ClassTable("shares.test", Collections.emptyList());

        metadataFactory.purge(tbl, ar -> {
            assertTrue(ar.succeeded());
            assertEquals(mockExecutor.expectedCalls.size(), mockExecutor.callCount, "All calls should be performed");
        });
    }

    private static DdlProperties getDdlProperties() {
        DdlProperties result = new DdlProperties();
        result.setTtlSec(3600);
        result.setCluster("test_arenadata");
        result.setArchiveDisk("default");
        return result;
    }
}