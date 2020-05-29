package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.EdmlRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class EdmlServiceImplTest {
	private static final String SELECT_1 = "SELECT Col1, Col2 FROM tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'" +
			" JOIN tbl2 FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' ON tbl1.Col3 = tbl2.Col4";
	private static final String INSERT_SELECT_1 = "INSERT INTO tblExt " + SELECT_1;

	private QueryExloadParam queryExloadParam;

	@Test
	void findTablesAfterInsertAndAfterFrom() {
		final String extTable = EdmlServiceImpl.extractExternalTable(INSERT_SELECT_1);
		assertEquals("tblExt", extTable);
	}

	@Test
	void cutOutInsertInto() {
		final String select = EdmlServiceImpl.cutOutInsertInto(INSERT_SELECT_1);
		assertEquals(SELECT_1, select);
	}

	@Test
	void execute() throws Throwable {
		VertxTestContext testContext = new VertxTestContext();

		final SchemaStorageProvider schemaProvider = mock(SchemaStorageProvider.class);
		Mockito.doAnswer(invocation -> {
			final Handler<AsyncResult<JsonObject>> handler = invocation.getArgument(1);
			handler.handle(Future.succeededFuture(new JsonObject()));
			return null;
		}).when(schemaProvider).getLogicalSchema(any(), any());

		final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginService.class);
		Mockito.when(dataSourcePluginService.getSourceTypes())
				.thenReturn(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG)));

		Mockito.doAnswer(invocation -> {
			queryExloadParam = ((MpprRequest) invocation.getArgument(1)).getQueryExloadParam();
			final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
			handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
			return null;
		}).when(dataSourcePluginService).mpprKafka(any(), any(MpprRequestContext.class), any());

		final ServiceDao serviceDao = mock(ServiceDao.class);
		Mockito.doAnswer(invocation -> {
			final Handler<AsyncResult<DownloadExtTableRecord>> handler = invocation.getArgument(2);
			final DownloadExtTableRecord detRecord = new DownloadExtTableRecord();
			detRecord.setId(1L);
			detRecord.setDatamart("test");
			detRecord.setTableName("tblExt");
			detRecord.setLocationType(Type.KAFKA_TOPIC);
			detRecord.setFormat(Format.AVRO);
			detRecord.setLocationPath("kafka://kafka-1.dtm.local:2181/test.datamart.test.c5aa558c07d144a6a6ddd9b3dea65c5f");
			handler.handle(Future.succeededFuture(detRecord));
			return null;
		}).when(serviceDao).findDownloadExternalTable(eq("test"), eq("tblExt"), any());
		Mockito.doAnswer(invocation -> {
			final Handler<AsyncResult<Void>> handler = invocation.getArgument(3);
			handler.handle(Future.succeededFuture());
			return null;
		}).when(serviceDao).insertDownloadQuery(any(), eq(1L), eq(SELECT_1), any());

		EdmlProperties edmlProperties = new EdmlProperties();
		edmlProperties.setSourceType(SourceType.ADB);
		final EdmlServiceImpl edmlService = new EdmlServiceImpl(
				dataSourcePluginService,
				serviceDao,
				schemaProvider,
				edmlProperties);

		final ParsedQueryRequest parsedQueryRequest = new ParsedQueryRequest();
		parsedQueryRequest.setProcessingType(SqlProcessingType.EDML);
		final QueryRequest queryRequest = new QueryRequest();
		queryRequest.setSql(INSERT_SELECT_1);
		queryRequest.setDatamartMnemonic("test");
		parsedQueryRequest.setQueryRequest(queryRequest);

		edmlService.execute(new EdmlRequestContext(new EdmlRequest(queryRequest)), testContext.completing());

		assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
		if (testContext.failed()) {
			throw testContext.causeOfFailure();
		}

		assertNotNull(queryExloadParam.getId());
		assertEquals("test", queryExloadParam.getDatamart());
		assertEquals("tblExt", queryExloadParam.getTableName());
		assertEquals(SELECT_1, queryExloadParam.getSqlQuery());

		verify(dataSourcePluginService, times(1)).mpprKafka(any(), any(), any());
	}

}
