package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.DeltaInformationExtractorImpl;
import io.arenadata.dtm.query.execution.core.calcite.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.configuration.properties.CoreDtmSettings;
import io.arenadata.dtm.query.execution.core.factory.RequestContextFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.QueryRequestFactoryImpl;
import io.arenadata.dtm.query.execution.core.factory.impl.RequestContextFactoryImpl;
import io.arenadata.dtm.query.execution.core.service.query.QueryAnalyzer;
import io.arenadata.dtm.query.execution.core.service.query.QueryDispatcher;
import io.arenadata.dtm.query.execution.core.service.query.impl.QueryAnalyzerImpl;
import io.arenadata.dtm.query.execution.core.service.query.impl.QuerySemicolonRemoverImpl;
import io.arenadata.dtm.query.execution.core.utils.DatamartMnemonicExtractor;
import io.arenadata.dtm.query.execution.core.utils.DefaultDatamartSetter;
import io.arenadata.dtm.query.execution.core.utils.HintExtractor;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.reactivex.ext.unit.Async;
import io.vertx.reactivex.ext.unit.TestSuite;
import lombok.Data;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;

import java.time.ZoneId;

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
    final CoreDtmSettings dtmSettings = new CoreDtmSettings(ZoneId.of("UTC"));
    private RequestContextFactory<CoreRequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory =
            new RequestContextFactoryImpl(new SqlDialect(SqlDialect.EMPTY_CONTEXT), dtmSettings, coreConfiguration);
    private QueryDispatcher queryDispatcher = mock(QueryDispatcher.class);
    private QueryAnalyzer queryAnalyzer;

    @BeforeEach
    void setUp() {
        queryAnalyzer = new QueryAnalyzerImpl(queryDispatcher,
                definitionService,
                requestContextFactory,
                vertx,
                new HintExtractor(),
                new DatamartMnemonicExtractor(new DeltaInformationExtractorImpl(dtmSettings)),
                new DefaultDatamartSetter(),
                new QuerySemicolonRemoverImpl(), new QueryRequestFactoryImpl(new AppConfiguration(mock(Environment.class))));
    }

    @Test
    void parsedSelect() {
        InputQueryRequest queryRequest = new InputQueryRequest();
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
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.PSO SELECT * FROM TEST_DATAMART.PSO");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.EDML, testData.getProcessingType());
    }

    @Test
    void parseSelectWithHintReturnsComplete() {
        InputQueryRequest queryRequest = new InputQueryRequest();
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
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("SELECT * FROM TEST_DATAMART.PSO for system_time" +
                " as of '2011-01-02 00:00:00' where 1=1");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DML, testData.getProcessingType());
    }

    @Test
    void parseEddl() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("DROP DOWNLOAD EXTERNAL TABLE test.s");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(queryRequest.getSql(), testData.getRequest().getSql());
        assertEquals(SqlProcessingType.EDDL, testData.getProcessingType());
    }

    @Test
    void parseDdl() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("DROP TABLE r.l");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DDL, testData.getProcessingType());
    }

    @Test
    void parseAlterView() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("ALTER VIEW test.view_a AS SELECT * FROM test.test_data");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DDL, testData.getProcessingType());
    }

    private void analyzeAndExecute(TestData testData, InputQueryRequest queryRequest) {
        TestSuite suite = TestSuite.create("parse");
        suite.test("parse", context -> {
            Async async = context.async();
            queryAnalyzer.analyzeAndExecute(queryRequest)
                    .onComplete(res -> {
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
            final CoreRequestContext ddlRequest = invocation.getArgument(0);
            testData.setRequest(ddlRequest.getRequest().getQueryRequest());
            testData.setProcessingType(ddlRequest.getProcessingType());
            return Future.succeededFuture(QueryResult.emptyResult());
        }).when(queryDispatcher).dispatch(any());
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
