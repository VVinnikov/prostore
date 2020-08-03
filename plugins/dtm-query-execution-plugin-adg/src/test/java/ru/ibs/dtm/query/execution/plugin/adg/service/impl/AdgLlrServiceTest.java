package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.plugin.adg.model.QueryResultItem;
import ru.ibs.dtm.query.execution.plugin.adg.service.DtmTestConfiguration;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import ru.ibs.dtm.query.execution.plugin.adg.service.impl.dml.AdgLlrService;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = DtmTestConfiguration.class)
@ExtendWith(VertxExtension.class)
@EnabledIfEnvironmentVariable(named = "skipITs", matches = "false")
public class AdgLlrServiceTest {

	private QueryEnrichmentService enrichmentService = mock(QueryEnrichmentService.class);
	private QueryExecutorService executorService = mock(QueryExecutorService.class);
	private LlrService<QueryResult> llrService = new AdgLlrService(enrichmentService, executorService);

	@Test
	void testExecuteNotEmptyOk() {
		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setRequestId(UUID.randomUUID());
		queryRequest.setSql("select name from s");

		QueryResultItem queryResultItem = new QueryResultItem(
				Collections.singletonList(new ColumnMetadata("name", ColumnType.VARCHAR)),
				Collections.singletonList(Collections.singletonList("val")));
		QueryResult expectedResult = new QueryResult(
				queryRequest.getRequestId(),
				new JsonArray(Collections.singletonList(new JsonObject().put("name", "val"))));

		prepare(queryRequest, queryResultItem);

		llrService.execute(new LlrRequestContext(new LlrRequest(queryRequest, new ArrayList<>())), handler -> {
			assertTrue(handler.succeeded());
			assertEquals(expectedResult, handler.result());
		});
	}

	@Test
	void testExecuteEmptyOk() {
		QueryRequest queryRequest = new QueryRequest();
		queryRequest.setRequestId(UUID.randomUUID());
		queryRequest.setSql("select name from s");

		QueryResultItem queryResultItem = new QueryResultItem(
				Collections.singletonList(new ColumnMetadata("name", ColumnType.VARCHAR)),
				Collections.emptyList());
		QueryResult expectedResult = new QueryResult(
				queryRequest.getRequestId(),
				new JsonArray());

		prepare(queryRequest, queryResultItem);

		llrService.execute(new LlrRequestContext(new LlrRequest(queryRequest, new ArrayList<>())), handler -> {
			assertTrue(handler.succeeded());
			assertEquals(expectedResult, handler.result());
		});
	}

	private void prepare(QueryRequest queryRequest, QueryResultItem queryResultItem) {
		doAnswer(invocation -> {
			Handler<AsyncResult<QueryResultItem>> handler = invocation.getArgument(1);
			handler.handle(Future.succeededFuture(queryResultItem));
			return null;
		}).when(executorService).execute(any(), any());

		doAnswer(invocation -> {
			Handler<AsyncResult<String>> handler = invocation.getArgument(1);
			handler.handle(Future.succeededFuture(queryRequest.getSql()));
			return null;
		}).when(enrichmentService).enrich(any(), any());
	}
}
