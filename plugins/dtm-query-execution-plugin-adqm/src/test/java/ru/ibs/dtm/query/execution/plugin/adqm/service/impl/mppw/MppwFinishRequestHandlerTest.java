package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

class MppwFinishRequestHandlerTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());

    @BeforeAll
    public static void setup() {
        ddlProperties.setTtlSec(3600);
        ddlProperties.setCluster("test_arenadata");
        ddlProperties.setArchiveDisk("default");
    }

    @Test
    public void testFinishRequestCallOrder() {
        Map<Predicate<String>, JsonArray> mockData = new HashMap<>();
        mockData.put(t -> t.contains(" from system.columns"), new JsonArray(Arrays.asList(
                new JsonObject("{\"name\": \"column1\"}"),
                new JsonObject("{\"name\": \"column2\"}"),
                new JsonObject("{\"name\": \"column3\"}"),
                new JsonObject("{\"name\": \"sys_from\"}"),
                new JsonObject("{\"name\": \"sys_to\"}"),
                new JsonObject("{\"name\": \"sys_op\"}"),
                new JsonObject("{\"name\": \"close_date\"}"),
                new JsonObject("{\"name\": \"sign\"}")
        )));

        mockData.put(t -> t.contains("select sorting_key from system.tables"), new JsonArray(Collections.singletonList(
                new JsonObject("{\"sorting_key\": \"column1, column2\"}")
        )));

        DatabaseExecutor executor = new MockDatabaseExecutor(Arrays.asList(
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_buffer"),
                t -> t.equalsIgnoreCase("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_actual"),
                t -> t.contains("a.column1, a.column2, a.column3, a.sys_from, 101") && t.contains("dev__shares.accounts_actual") &&
                        t.contains("ANY INNER JOIN dev__shares.accounts_buffer_shard b USING(column1, column2)") &&
                        t.contains("sys_from < 101"),
                t -> t.contains("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_actual"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("OPTIMIZE TABLE dev__shares.accounts_actual_shard ON CLUSTER test_arenadata FINAL")
        ), mockData);

        MppwRequestHandler handler = new MppwFinishRequestHandler(executor, ddlProperties, appConfiguration);
        QueryLoadParam loadParam = new QueryLoadParam();
        loadParam.setDatamart("shares");
        loadParam.setTableName("accounts");
        loadParam.setDeltaHot(101L);

        MppwRequest request = new MppwRequest(null, loadParam, new JsonObject());

        handler.execute(request).onComplete(ar -> Assertions.assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().getMessage() : ""));
    }
}