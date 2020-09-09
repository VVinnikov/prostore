package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
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
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.AttributeType;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;
import ru.ibs.dtm.query.execution.model.metadata.TableAttribute;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.AdqmHelperTableNamesFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.AdqmSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query.QueryRewriter;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class AdqmQueryEnrichmentServiceImplTest {
    private final QueryEnrichmentService oldEnrichService;
    private final QueryEnrichmentService enrichService;

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

        enrichService = new Adqm2QueryEnrichmentServiceImpl(
            queryParserService,
            contextProvider,
            new AdqmQueryGeneratorImpl(queryExtendService,
                calciteConfiguration.adgSqlDialect()),
            new AdqmSchemaExtenderImpl(helperTableNamesFactory));

        oldEnrichService = new AdqmQueryEnrichmentServiceImpl(
            queryParserService,
            contextProvider,
            new QueryRewriter(contextProvider, new AppConfiguration(new MockEnvironment()))
        );

    }

    private static void assertGrep(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(data);
        assertTrue(matcher.find(), String.format("Expected: %s, Received: %s", regexp, data));
    }

    @Test
    void enrichWithDeltaNum() {
        enrich(prepareRequestDeltaNum("SELECT a1.account_id\n" +
                "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id\n" +
                "WHERE a1.account_id = 1"),
            Arrays.asList("accounts"), enrichService);
    }

    @Test
    void enrichWithDeltaNum2() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                " join shares.transactions t on t.account_id = a.account_id" +
                " where a.account_id = 10"),
            Arrays.asList("accounts"), enrichService);
    }

    @Test
    void enrichWithDeltaNum3() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                " join shares.transactions t on t.account_id = a.account_id"),
            Arrays.asList("accounts"), enrichService);
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
            Arrays.asList("accounts"), enrichService);
    }

    @Test
    void enrichWithDeltaNum5() {
        enrich(prepareRequestDeltaNum("SELECT * FROM shares.transactions as tran"),
            Arrays.asList("accounts"), enrichService);
    }

    @Test
    void enrichWithDeltaNum6() {
        enrich(prepareRequestDeltaNum("SELECT a1.account_id\n" +
                "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id"),
            Arrays.asList("accounts"), enrichService);
    }

    @Test
    void enrichWithDeltaNumOld() {
        enrich(prepareRequestDeltaNum("SELECT a.account_id FROM shares.accounts a" +
                " join shares.transactions t on t.account_id = a.account_id" +
                " where a.account_id is not null"),
            Arrays.asList("accounts"), oldEnrichService);
    }

    @Test
    void enrichWithDeltaNumOld2() {
        enrich(prepareRequestDeltaNum("select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions t using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x"),
            Arrays.asList("accounts"), oldEnrichService);
    }

    @Test
    void enrichWithDeltaIntervalOld() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
            "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions t using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x");
        log.info(enrichQueryRequest.getQueryRequest().getSql());
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            enrichService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    log.info("SQL: " + result[0]);
//                    assertGrep(result[0], "sys_from >= 1 AND sys_from <= 5");
//                    assertGrep(result[0], "sys_to <= 3 AND sys_op = 1");
//                    assertGrep(result[0], "sys_to >= 2");
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @SneakyThrows
    private void enrich(EnrichQueryRequest enrichRequest,
                        List<String> expectedValues,
                        QueryEnrichmentService service) {
        String[] sqlResult = {""};

        val testContext = new VertxTestContext();
        testContext.assertFailure(Future.future((Promise<String> p) -> service.enrich(enrichRequest, p)));
//        service.enrich(enrichRequest, ar -> {
//            if (ar.succeeded()) {
//                sqlResult[0] = ar.result();
//                expectedValues.forEach(v -> assertGrep(sqlResult[0], v));
//                log.info("SQL: \n" + sqlResult[0]);
//                testContext.completeNow();
//            } else {
//
//                testContext.failNow(new RuntimeException("error"));
//            }
//        });
        assertThat(testContext.awaitCompletion(95, TimeUnit.SECONDS)).isTrue();
    }

    private EnrichQueryRequest prepareRequestMultipleSchema(String sql) {
        List<Datamart> datamarts = Arrays.asList(
            getSchema("shares", true),
            getSchema("shares_2", false),
            getSchema("test_datamart", false));
        String defaultSchema = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setSystemName("local");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(defaultSchema);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            new DeltaInformation("a", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, defaultSchema,
                datamarts.get(0).getDatamartTables().get(0).getLabel(), pos),
            new DeltaInformation("aa", "2019-12-23 15:15:14", false,
                2L, null, DeltaType.NUM, datamarts.get(1).getMnemonic(),
                datamarts.get(1).getDatamartTables().get(1).getLabel(), pos),
            new DeltaInformation("t", "2019-12-23 15:15:14", false,
                2L, null, DeltaType.NUM, datamarts.get(2).getMnemonic(),
                datamarts.get(2).getDatamartTables().get(1).getLabel(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts);
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = Arrays.asList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setSystemName("local");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schemaName);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            new DeltaInformation("a", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, schemaName, datamarts.get(0).getDatamartTables().get(0).getLabel(), pos),
            new DeltaInformation("t", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, schemaName, datamarts.get(0).getDatamartTables().get(1).getLabel(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts);
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Arrays.asList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setSystemName("local");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schemaName);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            new DeltaInformation("a", null, false,
                1L, new DeltaInterval(1L, 5L), DeltaType.STARTED_IN,
                schemaName, datamarts.get(0).getDatamartTables().get(0).getLabel(), pos),
            new DeltaInformation("t", null, false,
                1L, new DeltaInterval(3L, 4L), DeltaType.FINISHED_IN,
                schemaName, datamarts.get(0).getDatamartTables().get(1).getLabel(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts);
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private Datamart getSchema(String schemaName, boolean isDefault) {
        DatamartTable accounts = new DatamartTable();
        accounts.setLabel("accounts");
        accounts.setMnemonic("accounts");
        accounts.setDatamartMnemonic(schemaName);
        List<TableAttribute> accAttrs = new ArrayList<>();
        accAttrs.add(new TableAttribute(UUID.randomUUID(), "account_id", new AttributeType(UUID.randomUUID(),
            ColumnType.BIGINT), 0, 0, 1, 1, 1, false));
        accAttrs.add(new TableAttribute(UUID.randomUUID(), "account_type", new AttributeType(UUID.randomUUID(),
            ColumnType.VARCHAR), 1, 0, null, null, 2, false));
        accounts.setTableAttributes(accAttrs);
        DatamartTable transactions = new DatamartTable();
        transactions.setLabel("transactions");
        transactions.setMnemonic("transactions");
        transactions.setDatamartMnemonic(schemaName);
        List<TableAttribute> trAttr = new ArrayList<>();
        trAttr.add(new TableAttribute(UUID.randomUUID(), "transaction_id", new AttributeType(UUID.randomUUID(),
            ColumnType.BIGINT), 0, 0, 1, 1, 1, false));
        trAttr.add(new TableAttribute(UUID.randomUUID(), "transaction_date", new AttributeType(UUID.randomUUID(),
            ColumnType.DATE), 0, 0, null, null, 2, false));
        trAttr.add(new TableAttribute(UUID.randomUUID(), "account_id", new AttributeType(UUID.randomUUID(),
            ColumnType.BIGINT), 0, 0, 2, 1, 3, false));
        trAttr.add(new TableAttribute(UUID.randomUUID(), "amount", new AttributeType(UUID.randomUUID(),
            ColumnType.BIGINT), 0, 0, null, null, 4, true));
        transactions.setTableAttributes(trAttr);
        return new Datamart(UUID.randomUUID(), schemaName, isDefault, Arrays.asList(accounts, transactions));
    }

}
