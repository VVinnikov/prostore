package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import lombok.SneakyThrows;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaInterval;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.AdqmSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryRewriterTest {
    private static AppConfiguration appConfiguration;
    private static AdqmCalciteContextProvider calciteContextProvider;

    @BeforeAll
    public static void setup() {
        CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory()
        );

        calciteContextProvider = new AdqmCalciteContextProvider(
                parserConfig,
                new AdqmCalciteSchemaFactory(new AdqmSchemaFactory()));

        appConfiguration = new AppConfiguration(new MockEnvironment());
    }

    private static void assertGrep(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(data);
        assertTrue(matcher.find(), String.format("Expected: %s, Received: %s", regexp, data));
    }

    @Test
    void testQueryRewriter() {
        QueryRewriter rewriter = new QueryRewriter(calciteContextProvider, appConfiguration);
        String query = "SELECT a.account_id, a.account_type\n" +
                "FROM shares.accounts a\n" +
                "JOIN shares.transactions t ON t.account_id = a.account_id\n" +
                "WHERE a.account_id = 1 OR a.account_type = '100'";
        rewriter.rewrite(query, mockDeltas(), ar -> {
            assertTrue(ar.succeeded());
            String modifiedQuery = ar.result();

            // Physical names instead of datamart's
            assertGrep(modifiedQuery, "`dev__shares`.`accounts_actual`");
            assertGrep(modifiedQuery, "`dev__shares`.`transactions_actual_shard`");

            // Union all clause
            assertGrep(modifiedQuery, "UNION ALL");

            // where with delta filters
            assertGrep(modifiedQuery, "`a`.`sys_from` <= 98 AND `a`.`sys_to` >= 98");
            assertGrep(modifiedQuery, "`t`.`sys_from` <= 107 AND `t`.`sys_to` >= 107");
        });

    }

    private static List<DeltaInformation> mockDeltas() {
        SqlParserPos pos = new SqlParserPos(0, 0);
        return Arrays.asList(
                new DeltaInformation("a", "2019-12-23 15:15:14", false,
                        98L, null, DeltaType.NUM, "shares", "accounts", pos),
                new DeltaInformation("t", null, false,
                        107L, null, DeltaType.NUM, "shares", "transactions", pos)
        );
    }

    @Test
    public void testQueryRewriteDeltaNum() {
        QueryRewriter rewriter = new QueryRewriter(calciteContextProvider, appConfiguration);

        String query = "select *, " +
                "       CASE " +
                "         WHEN (account_type = 'D' AND amount >= 0) " +
                "              OR (account_type = 'C' AND  amount <= 0) THEN 'OK' " +
                "       ELSE 'NOT OK' " +
                "       END " +
                " from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, a.account_type\n" +
                "    from shares.accounts a " +
                "    join balances b on b.account_id = a.account_id\n" +
                "    left join shares.transactions" +
                "       using(account_id)\n" +
                "    group by a.account_id, a.account_type\n" +
                ") x";

        rewriter.rewrite(query, mockDeltasNumType(), ar -> {
            assertTrue(ar.succeeded());
            String modifiedQuery = ar.result();

            // Physical names instead of datamart's
            assertGrep(modifiedQuery, "`dev__shares`.`accounts_actual`");
            assertGrep(modifiedQuery, "`dev__test_datamart`.`balances_actual_shard`");
            assertGrep(modifiedQuery, "`dev__shares`.`transactions_actual_shard`");

            // Union all clause
            assertGrep(modifiedQuery, "UNION ALL");

            // where with delta filters
            assertGrep(modifiedQuery, "`a`.`sys_from` <= 101 AND `a`.`sys_to` >= 101");
            assertGrep(modifiedQuery, "`b`.`sys_from` <= 102 AND `b`.`sys_to` >= 102");
            assertGrep(modifiedQuery, "`transactions_actual_shard`.`sys_from` <= 103 AND `transactions_actual_shard`.`sys_to` >= 103");
        });
    }

    private static List<DeltaInformation> mockDeltasNumType() {
        SqlParserPos pos = new SqlParserPos(0, 0);
        return Arrays.asList(
                new DeltaInformation("a", "2019-12-23 15:15:14", false,
                        101L, null, DeltaType.NUM, "shares", "accounts", pos),
                new DeltaInformation("b", "2019-12-23 15:15:14", false,
                        102L, null, DeltaType.NUM, "test_datamart", "balances", pos),
                new DeltaInformation("", "2020-06-10 23:59:59", false,
                        103L, null, DeltaType.NUM, "shares", "transactions", pos)
        );
    }

    @Test
    public void testQueryRewriteDeltaInterval() {
        QueryRewriter rewriter = new QueryRewriter(calciteContextProvider, appConfiguration);

        String query = "select *, " +
                "       CASE " +
                "         WHEN (account_type = 'D' AND amount >= 0) " +
                "              OR (account_type = 'C' AND  amount <= 0) THEN 'OK' " +
                "       ELSE 'NOT OK' " +
                "       END " +
                " from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, a.account_type\n" +
                "    from shares.accounts a " +
                "    join balances b on b.account_id = a.account_id\n" +
                "    left join shares.transactions" +
                "       using(account_id)\n" +
                "    group by a.account_id, a.account_type\n" +
                ") x";

        rewriter.rewrite(query, mockDeltasIntervalType(), ar -> {
            assertTrue(ar.succeeded());
            String modifiedQuery = ar.result();

            // Physical names instead of datamart's
            assertGrep(modifiedQuery, "`dev__shares`.`accounts_actual`");
            assertGrep(modifiedQuery, "`dev__test_datamart`.`balances_actual_shard`");
            assertGrep(modifiedQuery, "`dev__shares`.`transactions_actual_shard`");

            // Union all clause
            assertGrep(modifiedQuery, "UNION ALL");

            // where with delta filters
            assertGrep(modifiedQuery, "`a`.`sys_from` >= 101 AND `a`.`sys_from` <= 102");
            assertGrep(modifiedQuery, "`b`.`sys_to` <= 103 AND `b`.`sys_op` = 1");
            assertGrep(modifiedQuery, "`b`.`sys_to` >= 102");
            assertGrep(modifiedQuery, "`transactions_actual_shard`.`sys_from` <= 105 AND `transactions_actual_shard`.`sys_to` >= 105");
        });
    }

    private static List<DeltaInformation> mockDeltasIntervalType() {
        SqlParserPos pos = new SqlParserPos(0, 0);
        return Arrays.asList(
                new DeltaInformation("a", null, false,
                        0L, new DeltaInterval(101L, 102L), DeltaType.STARTED_IN, "shares", "accounts", pos),
                new DeltaInformation("b", null, false,
                        0L, new DeltaInterval(103L, 104L), DeltaType.FINISHED_IN, "test_datamart", "balances", pos),
                new DeltaInformation("", "2020-06-10 23:59:59", false,
                        105L, null, DeltaType.NUM, "shares", "transactions", pos)
        );
    }

    @SneakyThrows
    @Test
    public void testFinalKeywordRewrite() {
        QueryRewriter rewriter = new QueryRewriter(calciteContextProvider, appConfiguration);

        String query = "select * from shares.account_actual_final t";
        SqlNode root = calciteContextProvider.context(null).getPlanner().parse(query);
        String result = rewriter.replaceFinalToKeyword(root.toString());

        assertGrep(result, "`account_actual`\\s+AS `t`\\s+FINAL");

        query = "select * from shares.account_actual_final";
        root = calciteContextProvider.context(null).getPlanner().parse(query);
        result = rewriter.replaceFinalToKeyword(root.toString());

        assertGrep(result, "`account_actual`\\s+FINAL");

        query = "select * from shares.account_actual_final a join shares.transactions_actual_final using(account_id)";
        root = calciteContextProvider.context(null).getPlanner().parse(query);
        result = rewriter.replaceFinalToKeyword(root.toString());

        assertGrep(result, "`account_actual`\\s+AS `a`\\s+FINAL");
        assertGrep(result, "`transactions_actual`\\s+FINAL\\s+USING");
    }

    // Default Parser implementation uses \" for quoting, but default internal formatting is backtick `
    // So when we translate SqlNode.toString, and try to parse it, we should use another parser config
    private SqlParser.Config internalRepresentationConfig() {
        return SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setCaseSensitive(false)
                .setQuotedCasing(Casing.UNCHANGED)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.BACK_TICK)
                .build();
    }

    @SneakyThrows
    private SqlNode parseInternalRepresentation(String sql) {
        SqlParser parser = SqlParser.create(sql, internalRepresentationConfig());
        return parser.parseQuery();
    }

}