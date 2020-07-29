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

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class AdbQueryEnrichmentServiceImplTest {

    QueryEnrichmentService adbQueryEnrichmentService;

    public AdbQueryEnrichmentServiceImplTest() {
        QueryExtendService queryExtender = new AdbCalciteDMLQueryExtendServiceImpl();

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
        String expectedResult = "SELECT t3.account_id,\n" +
                "CASE WHEN SUM(t8.amount) IS NOT NULL THEN CAST(SUM(t8.amount) AS BIGINT)\n" +
                "ELSE 0 END AS amount, t3.account_type,\n" +
                "CASE WHEN t3.account_type = 'D' AND CASE WHEN SUM(t8.amount) IS NOT NULL\n" +
                "THEN CAST(SUM(t8.amount) AS BIGINT)\n" +
                "ELSE 0 END >= 0 OR t3.account_type = 'C' AND CASE WHEN SUM(t8.amount) IS NOT NULL\n" +
                "THEN CAST(SUM(t8.amount) AS BIGINT) ELSE 0 END <= 0 THEN 'OK    ' ELSE 'NOT OK' END\n" +
                "FROM (SELECT account_id, account_type\n" +
                "\t\tFROM shares.accounts_history\n" +
                "\t\tWHERE sys_from <= 1 AND sys_to >= 1\n" +
                "\t\tUNION ALL\n" +
                "\t\tSELECT account_id, account_type\n" +
                "\t\tFROM shares.accounts_actual WHERE sys_from <= 1) AS t3\n" +
                "LEFT JOIN (SELECT transaction_id, transaction_date, account_id, amount\n" +
                "\t\t\tFROM shares.transactions_history\n" +
                "\t\tWHERE sys_from <= 1 AND sys_to >= 1\n" +
                "\t\tUNION ALL\n" +
                "\t\tSELECT transaction_id, transaction_date, account_id, amount\n" +
                "\t\tFROM shares.transactions_actual\n" +
                "\t\tWHERE sys_from <= 1) AS t8 ON t3.account_id = t8.account_id\n" +
                "GROUP BY t3.account_id, t3.account_type";
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
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
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertEquals(result[0].toLowerCase(), expectedResult.toLowerCase().replace("\t", "")
                .replace("\n", " "));
    }

    @Test
    void enrichWithDeltaInterval() {
        //FIXME завести баг с некорректным преобразованием значения: 'OK' -> 'OK    '
        String expectedResult = "SELECT t3.account_id,\n" +
                "CASE WHEN SUM(t8.amount) IS NOT NULL THEN CAST(SUM(t8.amount) AS BIGINT)\n" +
                "ELSE 0 END AS amount, t3.account_type, CASE WHEN t3.account_type = 'D' AND\n" +
                "CASE WHEN SUM(t8.amount) IS NOT NULL THEN CAST(SUM(t8.amount) AS BIGINT)\n" +
                "ELSE 0 END >= 0 OR t3.account_type = 'C' AND CASE WHEN SUM(t8.amount) IS NOT NULL\n" +
                "THEN CAST(SUM(t8.amount) AS BIGINT) ELSE 0 END <= 0\n" +
                "THEN 'OK    ' ELSE 'NOT OK' END\n" +
                "FROM (SELECT account_id, account_type\n" +
                "\tFROM shares.accounts_history\n" +
                "\tWHERE sys_from >= 1 AND sys_from <= 5\n" +
                "\tUNION ALL\n" +
                "\tSELECT account_id, account_type\n" +
                "\tFROM shares.accounts_actual\n" +
                "\tWHERE sys_from >= 1 AND sys_from <= 5) AS t3\n" +
                "LEFT JOIN (SELECT transaction_id, transaction_date, account_id, amount\n" +
                "\tFROM shares.transactions_history\n" +
                "\tWHERE sys_to >= 2 AND (sys_to <= 3 AND sys_op = 1)\n" +
                "\tUNION ALL\n" +
                "\tSELECT transaction_id, transaction_date, account_id, amount\n" +
                "\tFROM shares.transactions_actual\n" +
                "\tWHERE sys_to >= 2 AND (sys_to <= 3 AND sys_op = 1)) AS t8 ON t3.account_id = t8.account_id\n" +
                "GROUP BY t3.account_id, t3.account_type";
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
                }
                async.complete();
            });
            async.awaitSuccess(10000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertEquals(result[0].toLowerCase(), expectedResult.toLowerCase().replace("\t", "")
                .replace("\n", " "));
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = getSchema();
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
        List<Datamart> datamarts = getSchema();
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

    private List<Datamart> getSchema() {
        String schemaName = "shares";
        DatamartTable accounts = new DatamartTable();
        accounts.setLabel("accounts");
        accounts.setMnemonic("accounts");
        accounts.setDatamartMnemonic(schemaName);
        List<TableAttribute> accAttrs = new ArrayList<>();
        accAttrs.add(new TableAttribute(UUID.randomUUID(), "account_id", new AttributeType(UUID.randomUUID(),
                ColumnType.BIGINT), 0, 0, 1, 1));
        accAttrs.add(new TableAttribute(UUID.randomUUID(), "account_type", new AttributeType(UUID.randomUUID(),
                ColumnType.VARCHAR), 1, 0, null, null));
        accounts.setTableAttributes(accAttrs);
        DatamartTable transactions = new DatamartTable();
        transactions.setLabel("transactions");
        transactions.setMnemonic("transactions");
        transactions.setDatamartMnemonic(schemaName);
        List<TableAttribute> trAttr = new ArrayList<>();
        trAttr.add(new TableAttribute(UUID.randomUUID(), "transaction_id", new AttributeType(UUID.randomUUID(),
                ColumnType.BIGINT), 0, 0, 1, 1));
        trAttr.add(new TableAttribute(UUID.randomUUID(), "transaction_date", new AttributeType(UUID.randomUUID(),
                ColumnType.DATE), 0, 0, null, null));
        trAttr.add(new TableAttribute(UUID.randomUUID(), "account_id", new AttributeType(UUID.randomUUID(),
                ColumnType.BIGINT), 0, 0, 2, 1));
        trAttr.add(new TableAttribute(UUID.randomUUID(), "amount", new AttributeType(UUID.randomUUID(),
                ColumnType.BIGINT), 0, 0, null, null));
        transactions.setTableAttributes(trAttr);

        List<Datamart> datamarts = new ArrayList<>();
        datamarts.add(new Datamart(UUID.randomUUID(), schemaName, Arrays.asList(transactions, accounts)));
        return datamarts;
    }
}
