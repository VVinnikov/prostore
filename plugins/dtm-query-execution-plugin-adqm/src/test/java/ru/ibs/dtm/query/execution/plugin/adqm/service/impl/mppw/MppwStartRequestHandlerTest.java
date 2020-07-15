package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.query.execution.model.metadata.*;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class MppwStartRequestHandlerTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());
    private static final MppwProperties mppwProperties = new MppwProperties();

    @BeforeAll
    public static void setup() {
        ddlProperties.setTtlSec(3600);
        ddlProperties.setCluster("test_arenadata");
        ddlProperties.setArchiveDisk("default");

        mppwProperties.setConsumerGroup("adqm_group");
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
                t -> t.contains("CREATE TABLE dev__shares.accounts_ext_shard ON CLUSTER test_arenadata") &&
                        t.contains("column1 Nullable(Int64), column2 Nullable(Int64), column3 Nullable(String), sys_op Nullable(Int64)") &&
                        t.contains("ENGINE = Kafka()"),
                t -> t.equalsIgnoreCase("CREATE TABLE dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata (column1 Int64, column2 Int64, sys_op Nullable(Int8)) ENGINE = Join(ANY, INNER, column1, column2)"),
                t -> t.equalsIgnoreCase("CREATE TABLE dev__shares.accounts_buffer ON CLUSTER test_arenadata AS dev__shares.accounts_buffer_shard ENGINE=Distributed('test_arenadata', 'shares', 'accounts_buffer_shard', column1)"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_buffer\n" +
                        "  AS SELECT column1, column2, sys_op FROM dev__shares.accounts_ext_shard"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_actual\n" +
                        "AS SELECT column1, column2, column3, 101 AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op, '9999-12-31 00:00:00' as close_date, 1 AS sign  FROM dev__shares.accounts_ext_shard WHERE sys_op <> 1")
        ), mockData, false);

        MppwRequestHandler handler = new MppwStartRequestHandler(executor, ddlProperties, appConfiguration, mppwProperties);
        QueryLoadParam loadParam = createQueryLoadParam();
        JsonObject schema = createSchema();

        MppwRequest request = new MppwRequest(null, loadParam, schema);
        request.setTopic("adqm_topic");
        request.setZookeeperHost("zkhost");

        handler.execute(request).onComplete(ar -> Assertions.assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().getMessage() : ""));
    }

    private QueryLoadParam createQueryLoadParam() {
        val loadParam = new QueryLoadParam();
        loadParam.setDatamart("shares");
        loadParam.setTableName("accounts");
        loadParam.setDeltaHot(101L);

        return loadParam;
    }

    private JsonObject createSchema() {
        Datamart dm = new Datamart();
        dm.setMnemonic("shares");

        DatamartClass accounts = new DatamartClass();
        accounts.setMnemonic("accounts");
        ClassAttribute col1 = new ClassAttribute();
        col1.setMnemonic("column1");
        col1.setType(new TypeMessage(UUID.randomUUID(), ColumnType.INTEGER));
        ClassAttribute col2 = new ClassAttribute();
        col2.setMnemonic("column2");
        col2.setType(new TypeMessage(UUID.randomUUID(), ColumnType.INTEGER));
        ClassAttribute col3 = new ClassAttribute();
        col3.setMnemonic("column3");
        col3.setType(new TypeMessage(UUID.randomUUID(), ColumnType.STRING));
        accounts.setClassAttributes(Arrays.asList(col1, col2, col3));
        ClassAttribute col4 = new ClassAttribute();
        col4.setMnemonic("sys_op");
        col4.setType(new TypeMessage(UUID.randomUUID(), ColumnType.INTEGER));
        accounts.setClassAttributes(Arrays.asList(col1, col2, col3, col4));

        dm.setDatamartClassess(Collections.singletonList(accounts));

        return JsonObject.mapFrom(dm);
    }
}