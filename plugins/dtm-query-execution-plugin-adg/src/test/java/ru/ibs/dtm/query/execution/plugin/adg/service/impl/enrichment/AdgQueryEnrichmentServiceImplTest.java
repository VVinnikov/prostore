package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
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
import ru.ibs.dtm.query.execution.plugin.adg.calcite.AdgCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.AdgCalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.AdgCalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adg.factory.impl.AdgHelperTableNamesFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdgQueryEnrichmentServiceImplTest {

    private final QueryEnrichmentService enrichService;

    public AdgQueryEnrichmentServiceImplTest() {
        val calciteConfiguration = new AdgCalciteConfiguration();
        calciteConfiguration.init();
        val parserConfig = calciteConfiguration.configDdlParser(
            calciteConfiguration.ddlParserImplFactory()
        );
        val contextProvider = new AdgCalciteContextProvider(
            parserConfig,
            new AdgCalciteSchemaFactory(new AdgSchemaFactory()));

        val queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
        val helperTableNamesFactory = new AdgHelperTableNamesFactoryImpl();
        val queryExtendService = new AdgCalciteDmlQueryExtendServiceImpl(helperTableNamesFactory);

        enrichService = new AdgQueryEnrichmentServiceImpl(
            queryParserService,
            contextProvider,
            new AdgQueryGeneratorImpl(queryExtendService,
                calciteConfiguration.adgSqlDialect()),
            new AdgSchemaExtenderImpl(helperTableNamesFactory));
    }

    private static void assertGrep(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(data);
        assertTrue(matcher.find(), String.format("Expected: %s, Received: %s", regexp, data));
    }

    @Test
    void enrichWithDeltaNum() throws Throwable {
        enrich(prepareRequestDeltaNum("SELECT account_id FROM shares.accounts"),
            Arrays.asList("\"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1",
                "\"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1"));
    }

    @Test
    void enrichWithDeltaInterval() throws Throwable {
        enrich(prepareRequestDeltaInterval("select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
            "OR (account_type = 'C' AND  amount <= 0) THEN 'OK    ' ELSE 'NOT OK' END\n" +
            "  from (\n" +
            "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
            "    from shares.accounts a\n" +
            "    left join shares.transactions t using(account_id)\n" +
            "   group by a.account_id, account_type\n" +
            ")x"), Arrays.asList("\"local__shares__accounts_history\" where \"sys_from\" >= 1 and \"sys_from\" <= 5",
            "\"local__shares__accounts_actual\" where \"sys_from\" >= 1 and \"sys_from\" <= 5",
            "\"local__shares__transactions_history\" where \"sys_to\" >= 2",
            "\"sys_to\" <= 3 and \"sys_op\" = 1"));
    }

    @Test
    void enrichWithQuotes() throws Throwable {
        enrich(prepareRequestDeltaNum("SELECT \"account_id\" FROM \"shares\".\"accounts\""),
            Arrays.asList("\"local__shares__accounts_history\" where \"sys_from\" <= 1 and \"sys_to\" >= 1",
                "\"local__shares__accounts_actual\" where \"sys_from\" <= 1"));
    }

    @Test
    void enrichWithMultipleSchemas() throws Throwable {
        enrich(prepareRequestMultipleSchema("SELECT a.account_id FROM accounts a " +
                "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                "JOIN test_datamart.transactions t ON t.account_id = a.account_id"),
            Arrays.asList(
                "\"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1",
                "\"local__shares__accounts_actual\" where \"sys_from\" <= 1",
                "\"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2",
                "\"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2",
                "\"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2",
                "\"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2"));
    }

    private void enrich(EnrichQueryRequest enrichRequest, List<String> expectedValues) throws Throwable {
        String[] sqlResult = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            enrichService.enrich(enrichRequest, ar -> {
                if (ar.succeeded()) {
                    sqlResult[0] = ar.result();
                    expectedValues.forEach(v -> assertGrep(sqlResult[0], v));
                    async.complete();
                } else {
                    sqlResult[0] = "-1";
                }
            });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
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
                datamarts.get(0).getEntities().get(0).getName(), pos),
            new DeltaInformation("aa", "2019-12-23 15:15:14", false,
                2L, null, DeltaType.NUM, datamarts.get(1).getMnemonic(),
                datamarts.get(1).getEntities().get(1).getName(), pos),
            new DeltaInformation("t", "2019-12-23 15:15:14", false,
                2L, null, DeltaType.NUM, datamarts.get(2).getMnemonic(),
                datamarts.get(2).getEntities().get(1).getName(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts);
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setSystemName("local");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(schemaName);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
            new DeltaInformation("a", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, schemaName, datamarts.get(0).getEntities().get(0).getName(), pos),
            new DeltaInformation("t", "2019-12-23 15:15:14", false,
                1L, null, DeltaType.NUM, schemaName, datamarts.get(0).getEntities().get(1).getName(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts);
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
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
                schemaName, datamarts.get(0).getEntities().get(0).getName(), pos),
            new DeltaInformation("t", null, false,
                1L, new DeltaInterval(3L, 4L), DeltaType.FINISHED_IN,
                schemaName, datamarts.get(0).getEntities().get(1).getName(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, datamarts);
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
