package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.QueryEnrichmentProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.DeltaInformation;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.SchemaFactoryImpl;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryRewriterTest {
    private static final QueryEnrichmentProperties queryEnrichmentProperties = new QueryEnrichmentProperties();
    private static CalciteContextProvider calciteContextProvider;

    @BeforeAll
    public static void setup() {
        CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory()
        );

        calciteContextProvider = new CalciteContextProvider(
                parserConfig,
                new CalciteSchemaFactory(new SchemaFactoryImpl()));

        queryEnrichmentProperties.setDefaultDatamart("test_datamart");
        queryEnrichmentProperties.setEnvironment("dev");
    }

    @Test
    public void testQueryRewrite() {
        QueryRewriter rewriter = new QueryRewriter(calciteContextProvider, queryEnrichmentProperties);

        String query = "select *, " +
                "       CASE " +
                "         WHEN (account_type = 'D' AND amount >= 0) " +
                "              OR (account_type = 'C' AND  amount <= 0) THEN \"OK\" " +
                "       ELSE \"NOT OK\" " +
                "       END " +
                " from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, a.account_type\n" +
                "    from shares.accounts FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' a " +
                "    join balances b on b.account_id = a.account_id\n" +
                "    left join shares.transactions FOR SYSTEM_TIME AS OF '2020-06-10 23:59:59'" +
                "       using(account_id)\n" +
                "    group by a.account_id. a.account_type\n" +
                ") x";

        rewriter.rewrite(query, mockDeltas(), ar -> {
            assertTrue(ar.succeeded());
            String modifiedQuery = ar.result();
            assertGrep(modifiedQuery, "dev__shares.account_actual");
            assertGrep(modifiedQuery, "dev__test_datamart.balances_actual_shard");
            assertGrep(modifiedQuery, "dev__shares.transactions_actual_shard");
        });
    }

    @SneakyThrows
    @Test
    public void testFinalKeywordRewrite() {
        QueryRewriter rewriter = new QueryRewriter(calciteContextProvider, queryEnrichmentProperties);

        String query = "select * from shares.account_actual_final t";
        SqlNode root = calciteContextProvider.context(null).getPlanner().parse(query);
        String result = rewriter.replaceFinalToKeyword(root);

        assertGrep(result, "`account_actual`\\s+AS `t`\\s+FINAL");

        query = "select * from shares.account_actual_final";
        root = calciteContextProvider.context(null).getPlanner().parse(query);
        result = rewriter.replaceFinalToKeyword(root);

        assertGrep(result, "`account_actual`\\s+FINAL");

        query = "select * from shares.account_actual_final a join shares.transactions_actual_final using(account_id)";
        root = calciteContextProvider.context(null).getPlanner().parse(query);
        result = rewriter.replaceFinalToKeyword(root);

        assertGrep(result, "`account_actual`\\s+AS `a`\\s+FINAL");
        assertGrep(result, "`transactions_actual`\\s+FINAL\\s+USING");
    }

    private static void assertGrep(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp);
        Matcher matcher = pattern.matcher(data);
        assertTrue(matcher.find(), String.format("Expected: %s, Received: %s", regexp, data));
    }

    private static List<DeltaInformation> mockDeltas() {
        return Arrays.asList(
                new DeltaInformation("shares", "accounts", "a", "2019-12-23 15:15:14", 101L),
                new DeltaInformation("", "balances", "b", "2019-12-23 15:15:14", 102L),
                new DeltaInformation("shares", "transactions", "", "2020-06-10 23:59:59", 103L)
        );
    }
}