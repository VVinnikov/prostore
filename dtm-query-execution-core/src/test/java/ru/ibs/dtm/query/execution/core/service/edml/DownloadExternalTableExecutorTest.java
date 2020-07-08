package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.TableAttribute;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.transformer.Transformer;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.delta.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.impl.DeltaServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadExtTableAttributeDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadQueryDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.EddlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.UploadQueryDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.DownloadExtTableAttributeDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.DownloadQueryDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.EddlServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.UploadQueryDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExternalTableAttribute;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.edml.impl.DownloadExternalTableExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.impl.DownloadKafkaExecutor;
import ru.ibs.dtm.query.execution.core.service.impl.CalciteDefinitionService;
import ru.ibs.dtm.query.execution.core.transformer.DownloadExtTableAttributeTransformer;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DownloadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final EddlServiceDao eddlServiceDao = mock(EddlServiceDaoImpl.class);
    private final DownloadQueryDao downloadQueryDao = mock(DownloadQueryDaoImpl.class);
    private final DownloadExtTableAttributeDao downloadExtTableAttributeDao = mock(DownloadExtTableAttributeDaoImpl.class);
    private final Transformer<DownloadExternalTableAttribute, TableAttribute> tableAttributeTransformer = mock(DownloadExtTableAttributeTransformer.class);
    private final EdmlProperties edmlProperties = mock(EdmlProperties.class);
    private final List<EdmlDownloadExecutor> downloadExecutors = Arrays.asList(mock(DownloadKafkaExecutor.class));
    private DownloadExternalTableExecutor downloadExternalTableExecutor;
    private CalciteConfiguration config = new CalciteConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));
    private QueryRequest queryRequest;
    private DownloadExtTableRecord downRecord;

    @BeforeEach
    void setUp() {
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(tableAttributeTransformer,
                edmlProperties, serviceDbFacade, downloadExecutors);
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId("6efad624-b9da-4ba1-9fed-f2da478b08e8");

        downRecord = new DownloadExtTableRecord();
        downRecord.setId(1L);
        downRecord.setDatamartId(1L);
        downRecord.setTableName("download_table");
        downRecord.setFormat(Format.AVRO);
        downRecord.setLocationType(Type.KAFKA_TOPIC);
        downRecord.setLocationPath("kafka://kafka-1.dtm.local:9092/topic");
        downRecord.setChunkSize(1000);
        when(serviceDbFacade.getEddlServiceDao()).thenReturn(eddlServiceDao);
        when(eddlServiceDao.getDownloadQueryDao()).thenReturn(downloadQueryDao);
        when(eddlServiceDao.getDownloadExtTableAttributeDao()).thenReturn(downloadExtTableAttributeDao);
    }

    @Test
    void executeKafkaExecutorSuccess() throws Exception {
        Promise promise = Promise.promise();
        String selectSql = "select id, lst_nam FROM test.pso";
        String insertSql = "insert into test.download_table " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setTargetTable(new TableInfo("test", "download_table"));
        context.setSourceTable(new TableInfo("test", "pso"));

        EdmlQuery edmlQuery = new EdmlQuery(EdmlAction.DOWNLOAD, downRecord);
        List<DownloadExternalTableAttribute> attrs = new ArrayList<>();
        attrs.add(new DownloadExternalTableAttribute("id", "integer", 0, downRecord.getId()));
        attrs.add(new DownloadExternalTableAttribute("lst_name", "varchar(100)", 1, downRecord.getId()));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(3);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(downloadQueryDao).insertDownloadQuery(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<DownloadExternalTableAttribute>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(attrs));
            return null;
        }).when(downloadExtTableAttributeDao).findDownloadExtTableAttributes(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(downloadExecutors.get(0)).execute(any(), any());

        downloadExternalTableExecutor.execute(context, edmlQuery, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(context.getExloadParam().getChunkSize(), downRecord.getChunkSize());
        assertEquals(context.getExloadParam().getSqlQuery().replace("\n", " ").toLowerCase(), selectSql.toLowerCase());
        assertEquals(context.getExloadParam().getLocationPath(), downRecord.getLocationPath());
        assertEquals(context.getExloadParam().getLocationType(), downRecord.getLocationType());
        assertEquals(context.getExloadParam().getTableName(), context.getTargetTable().getTableName());
    }

    @Test
    void executeKafkaExecutorError() throws Exception {
        Promise promise = Promise.promise();
        String selectSql = "select id, lst_nam FROM test.pso";
        String insertSql = "insert into test.download_table " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setTargetTable(new TableInfo("test", "download_table"));
        context.setSourceTable(new TableInfo("test", "pso"));

        EdmlQuery edmlQuery = new EdmlQuery(EdmlAction.DOWNLOAD, downRecord);
        List<DownloadExternalTableAttribute> attrs = new ArrayList<>();
        attrs.add(new DownloadExternalTableAttribute("id", "integer", 0, downRecord.getId()));
        attrs.add(new DownloadExternalTableAttribute("lst_name", "varchar(100)", 1, downRecord.getId()));

        RuntimeException exception = new RuntimeException("Ошибка добавления download_query!");
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(exception));
            return null;
        }).when(downloadQueryDao).insertDownloadQuery(any(), any());

        downloadExternalTableExecutor.execute(context, edmlQuery, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(exception, promise.future().cause());
    }
}