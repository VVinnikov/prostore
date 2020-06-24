package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExternalTableAttribute;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.core.transformer.DownloadExtTableAttributeTransformer;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.EdmlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class EdmlServiceImplTest {
    private static final String SELECT_1 = "SELECT col1, col2\n" +
            "FROM tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
            "INNER JOIN tbl2 FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' ON tbl1.col3 = tbl2.col4";
    private static final String EXPECTED_EXT_TABLE = "tbl_ext";
    private static final String INSERT_SELECT_1 = "INSERT INTO " + EXPECTED_EXT_TABLE + " " + SELECT_1;
    public static final DownloadExternalTableAttribute EXPECTED_ATTRIBUTE = new DownloadExternalTableAttribute(
            "column1",
            "varchar",
            1,
            1L
    );
    private CalciteConfiguration config = new CalciteConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));

    private QueryExloadParam queryExloadParam;

    @Test
    void execute() throws Throwable {
		//TODO переписать тест
        VertxTestContext testContext = new VertxTestContext();

        val attributeMapper = new DownloadExtTableAttributeTransformer();

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
            queryExloadParam = ((MpprRequestContext) invocation.getArgument(1)).getRequest().getQueryExloadParam();
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(dataSourcePluginService).mpprKafka(any(), any(MpprRequestContext.class), any());

        final ServiceDao serviceDao = mock(ServiceDao.class);
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DownloadExtTableRecord>> handler = invocation.getArgument(2);
            final DownloadExtTableRecord detRecord = new DownloadExtTableRecord();
            detRecord.setId(1L);
            detRecord.setDatamartId(1L);
            detRecord.setTableName(EXPECTED_EXT_TABLE);
            detRecord.setLocationType(Type.KAFKA_TOPIC);
            detRecord.setFormat(Format.AVRO);
            detRecord.setLocationPath("kafka://kafka-1.dtm.local:2181/test.datamart.test.c5aa558c07d144a6a6ddd9b3dea65c5f");
            handler.handle(Future.succeededFuture(detRecord));
            return null;
        }).when(serviceDao).findDownloadExternalTable(eq("test"), eq(EXPECTED_EXT_TABLE), any());
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(3);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(serviceDao).insertDownloadQuery(any(), eq(1L), eq(SELECT_1), any());
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DownloadExternalTableAttribute>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(Collections.singletonList(EXPECTED_ATTRIBUTE)));
            return null;
        }).when(serviceDao).findDownloadExternalTableAttributes(any(), any());
        List<EdmlExecutor> edmlExecutors = new ArrayList<>();
        EdmlProperties edmlProperties = new EdmlProperties();
        edmlProperties.setSourceType(SourceType.ADB);
        final EdmlServiceImpl edmlService = new EdmlServiceImpl(serviceDao, schemaProvider, edmlExecutors);
        final ParsedQueryRequest parsedQueryRequest = new ParsedQueryRequest();
        parsedQueryRequest.setProcessingType(SqlProcessingType.EDML);
        final QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(INSERT_SELECT_1);
        queryRequest.setDatamartMnemonic("test");
        parsedQueryRequest.setQueryRequest(queryRequest);

        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(INSERT_SELECT_1);
        EdmlRequestContext context = new EdmlRequestContext(new EdmlRequest(queryRequest), sqlNode);
        edmlService.execute(context, testContext.completing());

        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }

        assertNotNull(queryExloadParam.getId());
        assertEquals("test", queryExloadParam.getDatamart());
        assertEquals(EXPECTED_EXT_TABLE, queryExloadParam.getTableName());
        assertEquals(SELECT_1, queryExloadParam.getSqlQuery());
        assertEquals(1, queryExloadParam.getTableAttributes().size());
        assertEquals(attributeMapper.transform(EXPECTED_ATTRIBUTE), queryExloadParam.getTableAttributes().get(0));
        verify(dataSourcePluginService, times(1)).mpprKafka(any(), any(), any());
    }

}
