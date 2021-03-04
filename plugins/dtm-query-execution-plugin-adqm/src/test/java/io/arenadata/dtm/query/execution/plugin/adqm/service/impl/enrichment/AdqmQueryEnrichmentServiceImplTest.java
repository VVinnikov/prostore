package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.impl.AdqmSchemaFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class AdqmQueryEnrichmentServiceImplTest {
    private static final int TIMEOUT_SECONDS = 120;
    private static final String ENV_NAME = "local";
    private static final List<Datamart> LOADED_DATAMARTS = loadDatamarts();
    private final QueryEnrichmentService enrichService;
    private final String[] expectedSqls;

    @SneakyThrows
    public AdqmQueryEnrichmentServiceImplTest() {
        val calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        val parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory());
        val contextProvider = new AdqmCalciteContextProvider(
                parserConfig,
                new AdqmCalciteSchemaFactory(new AdqmSchemaFactory()));

        val queryParserService = new AdqmCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
        val helperTableNamesFactory = new AdqmHelperTableNamesFactoryImpl();
        val queryExtendService = new AdqmCalciteDmlQueryExtendServiceImpl(helperTableNamesFactory);

        enrichService = new AdqmQueryEnrichmentServiceImpl(
                queryParserService,
                contextProvider,
                new AdqmQueryGeneratorImpl(queryExtendService,
                        calciteConfiguration.adgSqlDialect()),
                new AdqmSchemaExtenderImpl(helperTableNamesFactory));

        expectedSqls = new String(Files.readAllBytes(Paths.get(getClass().getResource("/sql/expectedDmlSqls.sql").toURI())))
                .split("---");

    }

    @SneakyThrows
    private static List<Datamart> loadDatamarts() {
        return DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml.json"), new TypeReference<List<Datamart>>() {
                });
    }

    @SneakyThrows
    private static String loadTextFromFile(String path) {
        try (InputStream inputStream = AdqmQueryEnrichmentServiceImpl.class.getClassLoader().getResourceAsStream(path)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    @Test
    void enrichWithDeltaNum() {
        enrich(prepareRequestDeltaNum("SELECT a1.account_id\n" +
                        "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                        "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id\n" +
                        "WHERE a1.account_id = 1"),
                expectedSqls[0], enrichService);
    }

    @Test
    void enrichWithDeltaNum2() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10"),
                expectedSqls[1], enrichService);
    }

    @Test
    void enrichWithDeltaNum3() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id"),
                expectedSqls[2], enrichService);
    }

    @Test
    void enrichWithDeltaNum4() {
        enrich(prepareRequestDeltaNum("select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x"),
                expectedSqls[3], enrichService);
    }

    @Test
    void enrichWithDeltaNum5() {
        enrich(prepareRequestDeltaNum("SELECT * FROM shares.transactions as tran"),
                expectedSqls[4], enrichService);
    }

    @Test
    void enrichWithDeltaNum6() {
        enrich(prepareRequestDeltaNum("SELECT a1.account_id\n" +
                        "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                        "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id"),
                expectedSqls[5], enrichService);
    }

    @Test
    void enrichWithDeltaNum7() {
        enrich(prepareRequestDeltaNum("SELECT a1.account_id\n" +
                        "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                        "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id\n" +
                        "    INNER JOIN shares.transactions t2 ON a1.account_id = t2.transaction_id\n" +
                        "    INNER JOIN shares.transactions t3 ON a1.account_id = t3.transaction_id\n" +
                        "WHERE t1.account_id = 5 AND t2.transaction_id = 3"
                ),
                expectedSqls[6], enrichService);
    }

    @Test
    void enrichWithDeltaNum8() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id " +
                        "LIMIT 10"),
                expectedSqls[7], enrichService);
    }

    @Test
    void enrichCount() {
        enrich(prepareRequestDeltaNum("SELECT count(*) FROM shares.accounts"),
                expectedSqls[8], enrichService);
    }

    @Test
    void enrichWithDeltaNum9() {
        enrich(prepareRequestDeltaNum("SELECT * FROM shares.transactions where account_id = 1"),
                expectedSqls[9], enrichService);
    }

    @Test
    void enrichWithAggregate() {
        enrich(prepareRequestDeltaNumByAggregate("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "group by varchar_col\n" +
                        "order by varchar_col\n" +
                        "limit 2"),
                expectedSqls[10], enrichService);
    }

    @SneakyThrows
    private void enrich(EnrichQueryRequest enrichRequest,
                        String expectedSql,
                        QueryEnrichmentService service) {
        val testContext = new VertxTestContext();
        val actual = new String[]{""};
        service.enrich(enrichRequest)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        actual[0] = ar.result();
                        testContext.completeNow();
                    } else {
                        actual[0] = ar.cause().getMessage();
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertEquals(expectedSql.trim(), actual[0].trim(),
                String.format("Expected: %s\n Actual: %s", expectedSql.trim(), actual[0].trim()));
    }

    private EnrichQueryRequest prepareRequestDeltaNumByAggregate(String sql) {
        String schemaName = LOADED_DATAMARTS.get(0).getMnemonic();
        String tableName = LOADED_DATAMARTS.get(0).getEntities().get(0).getName();
        List<DeltaInformation> deltaInforamtions = Collections.singletonList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .build()
        );
        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .schema(LOADED_DATAMARTS)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        String tableName = datamarts.get(0).getEntities().get(0).getName();
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t1")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t2")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t3")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(tableName)
                        .pos(pos)
                        .build()
        );
        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .schema(datamarts)
                .build();
    }

    private Datamart getSchema(String schemaName, boolean isDefault) {
        Entity accounts = Entity.builder()
                .schema(schemaName)
                .name("accounts")
                .build();
        List<EntityField> accAttrs = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("account_id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.VARCHAR)
                        .name("account_type")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(1)
                        .build()
        );
        accounts.setFields(accAttrs);

        Entity transactions = Entity.builder()
                .schema(schemaName)
                .name("transactions")
                .build();

        List<EntityField> trAttr = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("transaction_id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.DATE)
                        .name("transaction_date")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(true)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("account_id")
                        .ordinalPosition(3)
                        .shardingOrder(1)
                        .primaryOrder(2)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("amount")
                        .ordinalPosition(4)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build()
        );

        transactions.setFields(trAttr);

        return new Datamart(schemaName, isDefault, Arrays.asList(transactions, accounts));
    }
}
