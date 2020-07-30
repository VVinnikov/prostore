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
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.service.QueryParserService;
import ru.ibs.dtm.query.execution.model.metadata.*;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.AdgCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.AdgCalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.AdgCalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adg.factory.impl.AdgHelperTableNamesFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

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

    private void enrich(EnrichQueryRequest enrichRequest, List<String> expectedValues) throws Throwable {
        String[] sqlResult = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            enrichService.enrich(enrichRequest, ar -> {
                if (ar.succeeded()) {
                    sqlResult[0] = ar.result();
                    expectedValues.forEach(v -> assertGrep(sqlResult[0], v));
                } else {
                    sqlResult[0] = "-1";
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = getSchema();
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
        List<Datamart> datamarts = getSchema();
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

    private List<Datamart> getSchema() {
        String schemaName = "shares";
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

        List<Datamart> datamarts = new ArrayList<>();
        datamarts.add(new Datamart(UUID.randomUUID(), schemaName, Arrays.asList(transactions, accounts)));
        return datamarts;
    }

    private static void assertGrep(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(data);
        assertTrue(matcher.find(), String.format("Expected: %s, Received: %s", regexp, data));
    }
}
