//package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.llr;
//
//import io.vertx.core.AsyncResult;
//import io.vertx.core.Future;
//import io.vertx.core.Handler;
//import io.vertx.core.json.JsonObject;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.calcite.rel.RelRoot;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.parser.SqlParseException;
//import org.apache.calcite.tools.RelConversionException;
//import org.apache.calcite.tools.ValidationException;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//import ru.ibs.dtm.common.dto.ActualDeltaRequest;
//import ru.ibs.dtm.common.reader.QueryRequest;
//import ru.ibs.dtm.common.service.DeltaService;
//import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContext;
//import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
//import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
//import ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query.parser.QueryParserService;
//import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
//
//import java.util.List;
//
//import static org.junit.jupiter.api.Assertions.assertNotNull;
//import static org.junit.jupiter.api.Assertions.fail;
//
//@Slf4j
//class LlrQueryRewriteServiceTest {
//    private static class MockDeltaService implements DeltaService {
//
//        @Override
//        public void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler) {
//            resultHandler.handle(Future.succeededFuture(101L));
//        }
//
//        @Override
//        public void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler) {
//
//        }
//    }
//
//    private static QueryParserService queryParserService;
//    private static CalciteContextProvider calciteContextProvider;
//
//    @BeforeAll
//    public static void setup() {
//        CalciteConfiguration cfg = new CalciteConfiguration();
//        cfg.init();
//        calciteContextProvider = new CalciteContextProvider(cfg.configDdlParser(cfg.ddlParserImplFactory()));
//        queryParserService = new QueryParserService(calciteContextProvider);
//    }
//
//    @Test
//    public void testSimpleQueryRewrite() {
//        DeltaService deltaService = new MockDeltaService();
//
//        LlrQueryRewriteService service = new LlrQueryRewriteService(deltaService, queryParserService);
//
//        QueryRequest request = new QueryRequest();
//        request.setSql("select * from test.pso");
//        LlrRequest testRequest = new LlrRequest(request, new JsonObject());
//
//        service.rewrite(testRequest, ar -> {
//            assertTrue(ar.succeeded());
//        });
//    }
//
//    @Test
//    public void testOriginalQueryParse() {
//        String testQuery = "SELECT Col1, Col2\n" +
//                "FROM tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' t\n" +
//                "JOIN tbl2 FOR SYSTEM_TIME AS OF '2020-06-10 23:59:59'\n" +
//                "ON t.Col3 = tbl2.Col4\n" +
//                "WHERE t.Col5 = 1 OR t.Col5 = 10";
//
//        queryParserService.parse(testQuery, ar -> {
//            assertTrue(ar.succeeded());
//            SqlNode root = ar.result();
//            CalciteContext context = calciteContextProvider.context();
//            try {
//                SqlNode root2 = context.getPlanner().parse(testQuery);
//                SqlNode validated = context.getPlanner().validate(root2);
//                RelRoot rootRel = context.getPlanner().rel(validated);
//                assertNotNull(rootRel);
//            } catch (RelConversionException | SqlParseException | ValidationException e) {
//                fail(e.getMessage());
//            }
//        });
//    }
//
//}