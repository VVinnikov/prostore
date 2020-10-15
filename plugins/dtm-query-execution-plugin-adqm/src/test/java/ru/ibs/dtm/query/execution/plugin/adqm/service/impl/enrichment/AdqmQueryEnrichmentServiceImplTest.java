package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaInterval;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.AdqmHelperTableNamesFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.AdqmSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class AdqmQueryEnrichmentServiceImplTest {
    private final QueryEnrichmentService enrichService;
    private final String[] expectedSqls;

    @SneakyThrows
    public AdqmQueryEnrichmentServiceImplTest() {
        val calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        val parserConfig = calciteConfiguration.configDdlParser(
            calciteConfiguration.ddlParserImplFactory()
        );
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

    @SneakyThrows
    private void enrich(EnrichQueryRequest enrichRequest,
                        String expectedSql,
                        QueryEnrichmentService service) {
        val testContext = new VertxTestContext();
        val actual = new String[]{""};
        service.enrich(enrichRequest, ar -> {
            if (ar.succeeded()) {
                actual[0] = ar.result();
                testContext.completeNow();
            } else {
                actual[0] = ar.cause().getMessage();
                testContext.failNow(ar.cause());
            }
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertEquals(expectedSql.trim(), actual[0].trim(),
                String.format("Expected: %s\n Actual: %s", expectedSql.trim(), actual[0].trim()));
    }

    private EnrichQueryRequest prepareRequestMultipleSchema(String sql) {
        List<Datamart> datamarts = Arrays.asList(
            getSchema("shares", true),
            getSchema("shares_2", false),
            getSchema("test_datamart", false));
        String defaultSchema = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setEnvName("local");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(defaultSchema);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            new DeltaInformation("a", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, defaultSchema,
                datamarts.get(0).getEntities().get(0).getName(), pos),
            new DeltaInformation("aa", "2019-12-23 15:15:14", false,
                2L, null, DeltaType.NUM, datamarts.get(1).getMnemonic(),
                datamarts.get(1).getEntities().get(1).getName(), pos),
            new DeltaInformation("t", "2019-12-23 15:15:14", false,
                2L, null, DeltaType.NUM, datamarts.get(2).getMnemonic(),
                datamarts.get(2).getEntities().get(1).getName(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts, Collections.emptyList());
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setEnvName("local");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schemaName);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            new DeltaInformation("a", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, schemaName, datamarts.get(0).getEntities().get(0).getName(), pos),
            new DeltaInformation("t1", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, schemaName, datamarts.get(0).getEntities().get(1).getName(), pos),
            new DeltaInformation("t2", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, schemaName, datamarts.get(0).getEntities().get(1).getName(), pos),
            new DeltaInformation("t3", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, schemaName, datamarts.get(0).getEntities().get(1).getName(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts, Collections.emptyList());
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setEnvName("local");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schemaName);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            new DeltaInformation("a", null, false,
                1L, new DeltaInterval(1L, 5L), DeltaType.STARTED_IN,
                schemaName, datamarts.get(0).getEntities().get(0).getName(), pos),
            new DeltaInformation("t", null, false,
                1L, new DeltaInterval(3L, 4L), DeltaType.FINISHED_IN,
                schemaName, datamarts.get(0).getEntities().get(1).getName(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts, Collections.emptyList());
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
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
