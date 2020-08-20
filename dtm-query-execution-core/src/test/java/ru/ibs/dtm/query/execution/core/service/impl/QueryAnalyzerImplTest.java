package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.reactivex.ext.unit.Async;
import io.vertx.reactivex.ext.unit.TestSuite;
import lombok.Data;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.factory.RequestContextFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.RequestContextFactoryImpl;
import ru.ibs.dtm.query.execution.core.service.QueryAnalyzer;
import ru.ibs.dtm.query.execution.core.service.QueryDispatcher;
import ru.ibs.dtm.query.execution.core.utils.DatamartMnemonicExtractor;
import ru.ibs.dtm.query.execution.core.utils.DefaultDatamartSetter;
import ru.ibs.dtm.query.execution.core.utils.HintExtractor;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class QueryAnalyzerImplTest {

    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private Vertx vertx = Vertx.vertx();
    private RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory = new RequestContextFactoryImpl(new SqlDialect(SqlDialect.EMPTY_CONTEXT));
    private QueryDispatcher queryDispatcher = mock(QueryDispatcher.class);
    private QueryAnalyzer queryAnalyzer = new QueryAnalyzerImpl(queryDispatcher,
            definitionService,
            requestContextFactory,
            vertx,
            new HintExtractor(),
            new DatamartMnemonicExtractor(),
            new AppConfiguration(mock(Environment.class)),
            new DefaultDatamartSetter(),
            new SemicolonRemoverImpl());

    @Test
    void parsedSelect() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("select count(*) from (SelEct * from TEST_DATAMART.PSO where LST_NAM='test' " +
                "union all " +
                "SelEct * from TEST_DATAMART.PSO where LST_NAM='test1') " +
                "group by ID " +
                "order by 1 desc");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DML, testData.getProcessingType());
    }

    @Test
    void parseInsertSelect() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.PSO SELECT * FROM TEST_DATAMART.PSO");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.EDML, testData.getProcessingType());
    }

    @Test
    void parseSelectWithHintReturnsComplete() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("SELECT * FROM TEST_DATAMART.PSO DATASOURCE_TYPE='ADB'");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals("test_datamart"
                , testData.getRequest().getDatamartMnemonic());
        assertEquals(SqlProcessingType.DML, testData.getProcessingType());
    }

    @Test
    void parseSelectForSystemTime() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("SELECT * FROM TEST_DATAMART.PSO for system_time" +
                " as of '2011-01-02 00:00:00' where 1=1");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DML, testData.getProcessingType());
    }

    @Test
    void parseEddl() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("DROP DOWNLOAD EXTERNAL TABLE test.s");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(queryRequest.getSql(), testData.getRequest().getSql());
        assertEquals(SqlProcessingType.EDDL, testData.getProcessingType());
    }

    @Test
    void parseDdl() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("DROP TABLE r.l");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DDL, testData.getProcessingType());
    }

    @Test
    void parseAlterView() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("ALTER VIEW test.view_a AS SELECT * FROM test.test_data");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DDL, testData.getProcessingType());
    }

    private void analyzeAndExecute(TestData testData, QueryRequest queryRequest) {
        TestSuite suite = TestSuite.create("parse");
        suite.test("parse", context -> {
            Async async = context.async();
            queryAnalyzer.analyzeAndExecute(queryRequest, res -> {
                testData.setResult("complete");
                async.complete();
            });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private TestData prepareExecute() {
        TestData testData = new TestData();
        doAnswer(invocation -> {
            final RequestContext ddlRequest = invocation.getArgument(0);
            testData.setRequest(ddlRequest.getRequest().getQueryRequest());
            testData.setProcessingType(ddlRequest.getProcessingType());
            Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(queryDispatcher).dispatch(any(), any());
        return testData;
    }

    @Data
    private static class TestData {

        private String result;
        private QueryRequest request;
        private SqlProcessingType processingType;

        String getResult() {
            return result;
        }

        void setResult(String result) {
            this.result = result;
        }
    }
}
