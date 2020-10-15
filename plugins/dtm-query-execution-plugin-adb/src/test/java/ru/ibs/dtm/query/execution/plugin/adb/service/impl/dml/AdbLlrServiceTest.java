package ru.ibs.dtm.query.execution.plugin.adb.service.impl.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import utils.JsonUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
public class AdbLlrServiceTest {

	private LlrService adbLLRService;
	private QueryEnrichmentService adbQueryEnrichmentService = mock(QueryEnrichmentService.class);
	private DatabaseExecutor adbDatabaseExecutor = mock(DatabaseExecutor.class);
	private DeltaService deltaService = mock(DeltaService.class);

	public AdbLlrServiceTest() {
		//Моки для успешного исполнения
		AsyncResult<Void> asyncResultEmpty = mock(AsyncResult.class);
		when(asyncResultEmpty.succeeded()).thenReturn(true);

		AsyncResult<List<List<?>>> asyncResult = mock(AsyncResult.class);
		when(asyncResult.succeeded()).thenReturn(true);
		when(asyncResult.result()).thenReturn(new ArrayList<>());
		doAnswer((Answer<AsyncResult<Void>>) arg0 -> {
			((Handler<AsyncResult<Void>>) arg0.getArgument(1)).handle(asyncResultEmpty);
			return null;
		}).when(adbQueryEnrichmentService).enrich(any(), any());
		doAnswer((Answer<AsyncResult<List<List<?>>>>) arg0 -> {
			((Handler<AsyncResult<List<List<?>>>>) arg0.getArgument(2)).handle(asyncResult);
			return null;
		}).when(adbDatabaseExecutor).execute(any(), any(), any());

		AsyncResult<Long> asyncResultDelta = mock(AsyncResult.class);
		when(asyncResultDelta.succeeded()).thenReturn(true);
		when(asyncResultDelta.result()).thenReturn(1L);
		doAnswer((Answer<AsyncResult<Long>>) arg0 -> {
			((Handler<AsyncResult<Long>>) arg0.getArgument(1)).handle(asyncResultDelta);
			return null;
		}).when(deltaService).getDeltaOnDateTime(any(), any());

		adbLLRService = new AdbLlrService(adbQueryEnrichmentService, adbDatabaseExecutor);
	}

	@Test
	void executeQuery() {
		List<String> result = new ArrayList<>();
		TestSuite suite = TestSuite.create("the_test_suite");
		suite.test("executeQuery", context -> {
			Async async = context.async();
			JsonObject jsonSchema = JsonUtils.init("meta_data.json", "TEST_DATAMART");
			List<Datamart> schema = new ArrayList<>();
			schema.add(jsonSchema.mapTo(Datamart.class));
			QueryRequest queryRequest = new QueryRequest();
			queryRequest.setSql("SELECT * from PSO");
			queryRequest.setRequestId(UUID.randomUUID());
			queryRequest.setDatamartMnemonic("TEST_DATAMART");
			LlrRequest llrRequest = new LlrRequest(queryRequest, schema, Collections.emptyList());
			adbLLRService.execute(new LlrRequestContext(llrRequest), ar -> {
				log.debug(ar.toString());
				result.add("OK");
				async.complete();
			});
			async.awaitSuccess(7000);
		});
		suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
		log.info(result.get(0));
	}
}
