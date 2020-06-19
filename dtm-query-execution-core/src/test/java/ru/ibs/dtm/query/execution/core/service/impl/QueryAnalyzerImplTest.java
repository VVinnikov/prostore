package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.reactivex.ext.unit.Async;
import io.vertx.reactivex.ext.unit.TestSuite;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;
import ru.ibs.dtm.query.execution.core.factory.RequestContextFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.RequestContextFactoryImpl;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.QueryAnalyzer;
import ru.ibs.dtm.query.execution.core.service.QueryDispatcher;
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
	private DefinitionService<SqlNode> definitionService =
			new CalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));
	private Vertx vertx = Vertx.vertx();
	private RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory = new RequestContextFactoryImpl();
	private QueryDispatcher queryDispatcher = mock(QueryDispatcher.class);
	private QueryAnalyzer queryAnalyzer = new QueryAnalyzerImpl(queryDispatcher,
			definitionService,
			requestContextFactory,
			vertx, new HintExtractor());

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
		assertEquals(SqlProcessingType.DML, testData.getParsedQueryRequests().getProcessingType());
	}

	@Test
	void parseInsertSelect() {
		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setSql("INSERT INTO TEST_DATAMART.PSO SELECT * FROM TEST_DATAMART.PSO");

		TestData testData = prepareExecute();
		analyzeAndExecute(testData, queryRequest);

		assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
		assertEquals(SqlProcessingType.EDML, testData.getParsedQueryRequests().getProcessingType());
	}

	@Test
	void parseSelectWithHintReturnsComplete() {
		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setSql("SELECT * FROM TEST_DATAMART.PSO DATASOURCE_TYPE=ADB");

		TestData testData = prepareExecute();
		analyzeAndExecute(testData, queryRequest);

		assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
		assertEquals(SqlProcessingType.DML, testData.getParsedQueryRequests().getProcessingType());
	}

	@Test
	void parseSelectForSystemTime() {
		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setSql("SELECT * FROM TEST_DATAMART.PSO for system_time" +
				" as of '2011-01-02 00:00:00' where 1=1");

		TestData testData = prepareExecute();
		analyzeAndExecute(testData, queryRequest);

		assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
		assertEquals(SqlProcessingType.DML, testData.getParsedQueryRequests().getProcessingType());
	}

	@Test
	void parseEddl() {
		String sql = "DROP DOWNLOAD EXTERNAL TABLE S";
		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setSql(sql);

		TestData testData = prepareExecute();
		analyzeAndExecute(testData, queryRequest);

		assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
		assertEquals(sql, testData.getParsedQueryRequests().getQueryRequest().getSql());
		assertEquals(SqlProcessingType.EDDL, testData.getParsedQueryRequests().getProcessingType());
	}

	@Test
	void parseDdl() {
		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setSql("DROP TABLE r.l");

		TestData testData = prepareExecute();
		analyzeAndExecute(testData, queryRequest);

		assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
		assertEquals(SqlProcessingType.DDL, testData.getParsedQueryRequests().getProcessingType());
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
			testData.setParsedQueryRequests(invocation.getArgument(0));
			Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
			handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
			return null;
		}).when(queryDispatcher).dispatch(any(), any());
		return testData;
	}

	private static class TestData {

		private String result;

		private ParsedQueryRequest parsedQueryRequests;

		String getResult() {
			return result;
		}

		void setResult(String result) {
			this.result = result;
		}

		ParsedQueryRequest getParsedQueryRequests() {
			return parsedQueryRequests;
		}

		void setParsedQueryRequests(ParsedQueryRequest parsedQueryRequests) {
			this.parsedQueryRequests = parsedQueryRequests;
		}
	}
}
