package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockStatusReporter;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MppwStartRequestHandlerTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());
    private static final MppwProperties mppwProperties = new MppwProperties();

    private static final String TEST_TOPIC = "adqm_topic";
    private static final String TEST_CONSUMER_GROUP = "adqm_group";

    @BeforeAll
    public static void setup() {
        ddlProperties.setTtlSec(3600);
        ddlProperties.setCluster("test_arenadata");
        ddlProperties.setArchiveDisk("default");

        mppwProperties.setConsumerGroup(TEST_CONSUMER_GROUP);
        mppwProperties.setKafkaBrokers("localhost:9092");
        mppwProperties.setLoadType("KAFKA");
    }

    @Test
    public void testStartCallOrder() {
        Map<Predicate<String>, JsonArray> mockData = new HashMap<>();
        mockData.put(t -> t.contains("select engine_full"), new JsonArray(Collections.singletonList(
                new JsonObject("{\"engine_full\": \"Distributed('test_arenadata', 'shares', 'accounts_actual_shard', column1)\"}")
        )));
        mockData.put(t -> t.contains("select sorting_key"), new JsonArray(Collections.singletonList(
                new JsonObject("{\"sorting_key\": \"column1, column2, sys_from\"}")
        )));

        DatabaseExecutor executor = new MockDatabaseExecutor(Arrays.asList(
                t -> t.contains("CREATE TABLE IF NOT EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata") &&
                        t.contains("column1 Nullable(Int64), column2 Nullable(Int64), column3 Nullable(String), sys_op Nullable(Int32)") &&
                        t.contains("ENGINE = Kafka()"),
                t -> t.equalsIgnoreCase("CREATE TABLE IF NOT EXISTS dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata (column1 Int64, column2 Int64, sys_op_buffer Nullable(Int8)) ENGINE = Join(ANY, INNER, column1, column2)"),
                t -> t.equalsIgnoreCase("CREATE TABLE IF NOT EXISTS dev__shares.accounts_buffer ON CLUSTER test_arenadata AS dev__shares.accounts_buffer_shard ENGINE=Distributed('test_arenadata', 'shares', 'accounts_buffer_shard', column1)"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_buffer\n" +
                        "  AS SELECT column1, column2, sys_op AS sys_op_buffer FROM dev__shares.accounts_ext_shard"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_actual\n" +
                        "AS SELECT column1, column2, column3, 101 AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op, '9999-12-31 00:00:00' as close_date, 1 AS sign  FROM dev__shares.accounts_ext_shard WHERE sys_op <> 1")
        ), mockData, false);

        MockStatusReporter mockReporter = getMockReporter();
        MppwRequestHandler handler = new MppwStartRequestHandler(executor, ddlProperties, appConfiguration, mppwProperties,
                mockReporter);
        QueryLoadParam loadParam = createQueryLoadParam();
        JsonObject schema = createSchema();

        MppwRequest request = new MppwRequest(null, loadParam, schema);
        request.setTopic(TEST_TOPIC);
        request.setZookeeperHost("zkhost");

        handler.execute(request).onComplete(ar -> {
            assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().getMessage() : "");
            assertTrue(mockReporter.wasCalled("start"));
        });
    }

    private MockStatusReporter getMockReporter() {
        Map<String, JsonObject> expected = new HashMap<>();
        expected.put("start", new JsonObject(format("{\"topic\": \"%s\", \"consumerGroup\": \"%s\"}", TEST_TOPIC,
                TEST_CONSUMER_GROUP + "dev__shares.accounts")));
        return new MockStatusReporter(expected);
    }

    private QueryLoadParam createQueryLoadParam() {
        val loadParam = new QueryLoadParam();
        loadParam.setDatamart("shares");
        loadParam.setTableName("accounts");
        loadParam.setDeltaHot(101L);

        return loadParam;
    }

    private JsonObject createSchema() {
        String jsonSchema = "{\"type\":\"record\",\"name\":\"accounts\",\"namespace\":\"dm2\",\"fields\":[{\"name\":\"column1\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"column2\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"column3\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";

        return new JsonObject(jsonSchema);
    }
}