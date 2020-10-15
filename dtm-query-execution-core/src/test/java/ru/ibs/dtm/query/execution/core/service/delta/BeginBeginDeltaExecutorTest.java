package ru.ibs.dtm.query.execution.core.service.delta;

import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.delta.QueryDeltaResult;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.delta.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.impl.DeltaServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.DeltaQueryResultFactoryImpl;
import ru.ibs.dtm.query.execution.core.service.delta.impl.BeginDeltaExecutor;
import ru.ibs.dtm.query.execution.core.utils.QueryResultUtils;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.BeginDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BeginBeginDeltaExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(DeltaQueryResultFactoryImpl.class);
    private BeginDeltaExecutor beginDeltaExecutor;
    private QueryRequest req = new QueryRequest();
    private DeltaRecord delta = new DeltaRecord();
    private String datamart;
    private String statusDate;

    @BeforeEach
    void beforeAll() {
        datamart = "test_datamart";
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setLoadId(0L);
        delta.setLoadProcId("load-proc-1");
        delta.setDatamartMnemonic(req.getDatamartMnemonic());
        statusDate = "2020-06-15T05:06:55";
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void executeDeltaWithInProgressStatus() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(null);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(createResult(statusDate, 0L));

        RuntimeException exception = new RuntimeException("Дельта находится в процессе загрузки!");
        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(exception));
            return null;
        }).when(deltaServiceDao).getDeltaHotByDatamart(any(), any());

        beginDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertEquals(exception, promise.future().cause());
    }

    @Test
    void executeDeltaWithErrorStatus() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        QueryDeltaResult res = new QueryDeltaResult(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME), 1L);

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(null);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(createResult(statusDate, 1L));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            delta.setStatus(DeltaLoadStatus.ERROR);//статус ошибка
            delta.setSinId(1L);
            handler.handle(Future.succeededFuture(delta));
            return null;
        }).when(deltaServiceDao).getDeltaHotByDatamart(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryDeltaResult));
            return null;
        }).when(deltaServiceDao).insertDelta(any(), any());

        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryDeltaResult);

        beginDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertEquals(res.getSinId(), ((QueryResult) promise.future().result()).getResult().get(0).get("sinId"));
    }

    @Test
    void executeDeltaWithSuccessStatus() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        QueryDeltaResult res = new QueryDeltaResult(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME), 2L);

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(null);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(createResult(statusDate, 2L));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            delta.setStatus(DeltaLoadStatus.SUCCESS);
            delta.setSinId(1L);
            handler.handle(Future.succeededFuture(delta));
            return null;
        }).when(deltaServiceDao).getDeltaHotByDatamart(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryDeltaResult));
            return null;
        }).when(deltaServiceDao).insertDelta(any(), any());

        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryDeltaResult);

        beginDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(res.getSinId(), ((QueryResult) promise.future().result()).getResult().get(0).get("sinId"));
    }

    @Test
    void executeDeltaWithNumSuccessStatus() {
        req.setSql("BEGIN DELTA 2");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(2L);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        RuntimeException exception = new RuntimeException("Номера заданной дельты и актуальной не совпадают!");

        QueryResult queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(createResult(statusDate, 2L));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(exception));
            return null;
        }).when(deltaServiceDao).getDeltaHotByDatamart(eq(datamart), any());

        beginDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(exception, promise.future().cause());
    }

    @Test
    void executeDeltaNotFound() {
        req.setSql("BEGIN DELTA 1");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        QueryDeltaResult res = new QueryDeltaResult(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME), 0L);

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(1L);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(createResult(statusDate, 0L));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(null));
            return null;
        }).when(deltaServiceDao).getDeltaHotByDatamart(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryDeltaResult));
            return null;
        }).when(deltaServiceDao).insertDelta(any(), any());

        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryDeltaResult);

        beginDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(res.getSinId(), ((QueryResult) promise.future().result()).getResult().get(0).get("sinId"));
    }

    private List<Map<String, Object>> createResult(String statusDate, Long sinId) {
        return QueryResultUtils.createResultWithSingleRow(Arrays.asList("statusDate", "sinId"), Arrays.asList(statusDate, sinId));
    }
}
