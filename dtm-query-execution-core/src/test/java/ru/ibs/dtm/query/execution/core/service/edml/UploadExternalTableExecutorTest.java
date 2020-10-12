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
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.EddlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.UploadQueryDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.EddlServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dao.eddl.impl.UploadQueryDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.HotDelta;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;
import ru.ibs.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import ru.ibs.dtm.query.execution.core.service.edml.impl.UploadKafkaExecutor;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UploadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final EddlServiceDao eddlServiceDao = mock(EddlServiceDaoImpl.class);
    private final UploadQueryDao uploadQueryDao = mock(UploadQueryDaoImpl.class);
    private final EdmlProperties edmlProperties = mock(EdmlProperties.class);
    private final List<EdmlUploadExecutor> uploadExecutors = Arrays.asList(mock(UploadKafkaExecutor.class));
    private UploadExternalTableExecutor uploadExternalTableExecutor;
    private CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private QueryRequest queryRequest;
    private UploadExtTableRecord uploadRecord;

    @BeforeEach
    void setUp() {
        uploadExternalTableExecutor = new UploadExternalTableExecutor(serviceDbFacade, edmlProperties, uploadExecutors);
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId("6efad624-b9da-4ba1-9fed-f2da478b08e8");
        uploadRecord = new UploadExtTableRecord();
        uploadRecord.setId(1L);
        uploadRecord.setDatamartId(1L);
        uploadRecord.setTableName("upload_table");
        uploadRecord.setFormat(Format.AVRO);
        uploadRecord.setLocationType(Type.KAFKA_TOPIC);
        uploadRecord.setLocationPath("kafka://kafka-1.dtm.local:9092/topic");
        uploadRecord.setMessageLimit(1000);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbFacade.getEddlServiceDao()).thenReturn(eddlServiceDao);
        when(eddlServiceDao.getUploadQueryDao()).thenReturn(uploadQueryDao);
    }

    @Test
    void executeKafkaExecutorSuccess() throws Exception {
        Promise promise = Promise.promise();
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        final HotDelta hotDelta = HotDelta.builder().deltaNum(1).build();

        EdmlQuery edmlQuery = new EdmlQuery(EdmlAction.UPLOAD, uploadRecord);

        Mockito.when(deltaServiceDao.getDeltaHot(any())).thenReturn(Future.succeededFuture(hotDelta));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(uploadQueryDao).inserUploadQuery(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            return null;
        }).when(uploadExecutors.get(0)).execute(any(), any());

        uploadExternalTableExecutor.execute(context, edmlQuery, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });

        assertEquals(context.getLoadParam().getDeltaHot(), hotDelta.getDeltaNum());
        assertEquals(context.getLoadParam().getTableName(), context.getTargetTable().getTableName());
        assertEquals(context.getLoadParam().getSqlQuery().replace("\n", " ").replace("\r", "").toLowerCase(), insertSql.toLowerCase());
        assertEquals(context.getLoadParam().getDatamart(), context.getRequest().getQueryRequest().getDatamartMnemonic());
        assertEquals(context.getLoadParam().getMessageLimit(), uploadRecord.getMessageLimit());
    }

    @Test
    void executeKafkaExecutorFindDeltaError() throws Exception {
        Promise promise = Promise.promise();
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(request, sqlNode);
        context.setTargetTable(new TableInfo("test", "pso"));
        context.setSourceTable(new TableInfo("test", "upload_table"));
        EdmlQuery edmlQuery = new EdmlQuery(EdmlAction.UPLOAD, uploadRecord);
        RuntimeException exception = new RuntimeException("Не найдена открытая дельта!");

        Mockito.when(deltaServiceDao.getDeltaHot(any())).thenReturn(Future.failedFuture(exception));

        uploadExternalTableExecutor.execute(context, edmlQuery, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(exception, promise.future().cause());
    }
}