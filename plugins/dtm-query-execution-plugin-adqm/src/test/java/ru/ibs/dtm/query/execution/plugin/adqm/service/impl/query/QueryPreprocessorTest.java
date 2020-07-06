package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.DeltaInformation;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.SchemaFactoryImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryPreprocessorTest {
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
    }

    @Test
    public void testFullQuery() {
        String query = "select *, " +
                "       CASE " +
                "         WHEN (account_type = 'D' AND amount >= 0) " +
                "              OR (account_type = 'C' AND  amount <= 0) THEN \"OK\" " +
                "       ELSE \"NOT OK\" " +
                "       END " +
                " from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, a.account_type\n" +
                "    from shares.accounts FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' a " +
                "    left join shares.transactions FOR SYSTEM_TIME AS OF '2020-06-10 23:59:59'" +
                "       using(account_id)\n" +
                "    group by a.account_id. a.account_type\n" +
                ") x";

        QueryPreprocessor preprocessor = new QueryPreprocessor(calciteContextProvider);
        preprocessor.process(query, ar -> {
            assertTrue(ar.succeeded());
            List<DeltaInformation> info = ar.result();
            assertEquals(2, info.size());

            DeltaInformation accounts = info.get(0);
            assertEquals("shares", accounts.getSchemaName());
            assertEquals("accounts", accounts.getTableName());
            assertEquals("a", accounts.getTableAlias());
            assertEquals("2019-12-23 15:15:14", accounts.getDeltaTimestamp());

            DeltaInformation transactions = info.get(1);
            assertEquals("shares", transactions.getSchemaName());
            assertEquals("transactions", transactions.getTableName());
            assertEquals("", transactions.getTableAlias());
            assertEquals("2020-06-10 23:59:59", transactions.getDeltaTimestamp());

        });
    }

    @Test
    public void testComplexQuery() {
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
                ")";

        QueryPreprocessor preprocessor = new QueryPreprocessor(calciteContextProvider);
        preprocessor.process(query, ar -> {
            assertTrue(ar.succeeded());
            List<DeltaInformation> info = ar.result();
            assertEquals(3, info.size());

            DeltaInformation accounts = info.get(0);
            assertEquals("shares", accounts.getSchemaName());
            assertEquals("accounts", accounts.getTableName());
            assertEquals("a", accounts.getTableAlias());
            assertEquals("2019-12-23 15:15:14", accounts.getDeltaTimestamp());

            DeltaInformation balances = info.get(1);
            assertEquals("", balances.getSchemaName());
            assertEquals("balances", balances.getTableName());
            assertEquals("b", balances.getTableAlias());
            assertFalse(StringUtils.isEmpty(balances.getDeltaTimestamp()));

            DeltaInformation transactions = info.get(2);
            assertEquals("shares", transactions.getSchemaName());
            assertEquals("transactions", transactions.getTableName());
            assertEquals("", transactions.getTableAlias());
            assertEquals("2020-06-10 23:59:59", transactions.getDeltaTimestamp());

        });
    }
}