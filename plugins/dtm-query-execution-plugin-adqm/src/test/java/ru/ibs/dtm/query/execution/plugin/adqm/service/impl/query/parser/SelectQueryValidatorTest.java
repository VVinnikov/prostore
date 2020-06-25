//package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query.parser;
//
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
//import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
//
//class SelectQueryValidatorTest {
//    private static QueryParserService queryParserService;
//    private static SelectQueryValidator selectQueryValidator;
//
//    @BeforeAll
//    public static void setup() {
//        CalciteConfiguration cfg = new CalciteConfiguration();
//        cfg.init();
//        CalciteContextProvider calciteContextProvider = new CalciteContextProvider(cfg.configDdlParser(cfg.ddlParserImplFactory()));
//
//        queryParserService = new QueryParserService(calciteContextProvider);
//        selectQueryValidator = new SelectQueryValidator();
//    }
//
//    void validateQuery(String query, boolean expectedValidatorResult) {
//        queryParserService.parse(query, ar -> {
//            assertTrue(ar.succeeded());
//            assertEquals(selectQueryValidator.test(ar.result()), expectedValidatorResult);
//        });
//    }
//
//    @Test
//    public void testQueryTypeAcceptance() {
//        String insertQuery = "insert into test(t1, t2) select t1, t2 from test2";
//        validateQuery(insertQuery, false);
//
//        String selectQuery = "select t1, t2 from test2";
//        validateQuery(selectQuery, true);
//
//        String selectAliasQuery = "select t1, t2 from test2 t";
//        validateQuery(selectAliasQuery, true);
//
//        String snapshotQuery = "select t1, t2 from test2 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t";
//        validateQuery(snapshotQuery, true);
//
//        String subquery = "select * from (select t1, t2 from test) t";
//        validateQuery(subquery, false);
//
//        String joinSubquery = "select t1, t2 from test t join (select t4, t5 from test2 t2) ts on t.t6 = ts.t4";
//        validateQuery(joinSubquery, false);
//    }
//}