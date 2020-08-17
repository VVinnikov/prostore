package ru.ibs.dtm.query.execution.plugin.adb.service.impl.dml;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaInterval;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.service.QueryParserService;
import ru.ibs.dtm.query.execution.model.metadata.*;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.AdbCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.AdbCalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.factory.impl.AdbSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryExtendService;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment.*;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class AdbQueryEnrichmentServiceImplTest {

    QueryEnrichmentService adbQueryEnrichmentService;

    public AdbQueryEnrichmentServiceImplTest() {
        QueryExtendService queryExtender = new AdbCalciteDmlQueryExtendServiceImpl();

        CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory()
        );

        AdbCalciteContextProvider contextProvider = new AdbCalciteContextProvider(
                parserConfig,
                new AdbCalciteSchemaFactory(new AdbSchemaFactory()));

        AdbQueryGeneratorImpl adbQueryGeneratorimpl = new AdbQueryGeneratorImpl(queryExtender, calciteConfiguration.adbSqlDialect());
        QueryParserService queryParserService = new AdbCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
        adbQueryEnrichmentService = new AdbQueryEnrichmentServiceImpl(
                queryParserService,
                adbQueryGeneratorimpl,
                contextProvider,
                new AdbSchemaExtenderImpl());

    }

    @Test
    void enrich() {
        List<String> result = new ArrayList<>();
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("select * from test_datamart.pso FOR SYSTEM_TIME AS OF TIMESTAMP '1999-01-08 04:05:06'");
        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                log.debug(ar.toString());
                result.add("OK");
                async.complete();
            });

            async.awaitSuccess(7000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        log.info(result.get(0));
    }

    @Test
    void enrichWithDeltaNum() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK   ' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    assertGrep(result[0], "sys_from <= 1 AND sys_to >= 1");
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enrichWithDeltaInterval() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    assertGrep(result[0], "sys_from >= 1 AND sys_from <= 5");
                    assertGrep(result[0], "sys_to <= 3 AND sys_op = 1");
                    assertGrep(result[0], "sys_to >= 2");
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void enfichWithMultipleLogicalSchema() {
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchemas(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id");
        String[] result = {""};

        TestSuite suite = TestSuite.create("the_test_suite");
        suite.test("executeQuery", context -> {
            Async async = context.async();
            adbQueryEnrichmentService.enrich(enrichQueryRequest, ar -> {
                if (ar.succeeded()) {
                    result[0] = ar.result();
                    assertGrep(result[0], "shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1");
                    assertGrep(result[0], "shares.accounts_actual WHERE sys_from <= 1");
                    assertGrep(result[0], "shares_2.accounts_history WHERE sys_from <= 1 AND sys_to >= 1");
                    assertGrep(result[0], "shares_2.accounts_actual WHERE sys_from <= 1");
                    assertGrep(result[0], "test_datamart.transactions_history WHERE sys_from <= 1 AND sys_to >= 1");
                    assertGrep(result[0], "test_datamart.transactions_actual WHERE sys_from <= 1");
                }
                async.complete();
            });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private EnrichQueryRequest prepareRequestMultipleSchemas(String sql){
        List<Datamart> schemas = Arrays.asList(
                getSchema("shares", true),
                getSchema("shares_2", false),
                getSchema("test_datamart", false));
        String requestSchema = schemas.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic(requestSchema);
        SqlParserPos pos = new SqlParserPos(0, 0);
        queryRequest.setDeltaInformations(Arrays.asList(
                new DeltaInformation("a", "2019-12-23 15:15:14", false,
                        1L, null, DeltaType.NUM, schemas.get(0).getMnemonic(), schemas.get(0).getDatamartTables().get(0).getLabel(), pos),
                new DeltaInformation("aa", "2019-12-23 15:15:14", false,
                        1L, null, DeltaType.NUM, schemas.get(1).getMnemonic(), schemas.get(1).getDatamartTables().get(1).getLabel(), pos),
                new DeltaInformation("t", "2019-12-23 15:15:14", false,
                        1L, null, DeltaType.NUM, schemas.get(2).getMnemonic(), schemas.get(2).getDatamartTables().get(1).getLabel(), pos)
        ));
        LlrRequest llrRequest = new LlrRequest(queryRequest, schemas);
        return EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = Arrays.asList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
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
                ColumnType.DATE), 0, 0, null, null, 2, true));
        trAttr.add(new TableAttribute(UUID.randomUUID(), "account_id", new AttributeType(UUID.randomUUID(),
                ColumnType.BIGINT), 0, 0, 2, 1, 3, false));
        trAttr.add(new TableAttribute(UUID.randomUUID(), "amount", new AttributeType(UUID.randomUUID(),
                ColumnType.BIGINT), 0, 0, null, null, 4, false));
        transactions.setTableAttributes(trAttr);

        return new Datamart(UUID.randomUUID(), schemaName, isDefault, Arrays.asList(transactions, accounts));
    }

    private static void assertGrep(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(data);
        assertTrue(matcher.find(), String.format("Expected: %s, Received: %s", regexp, data));
    }
}
