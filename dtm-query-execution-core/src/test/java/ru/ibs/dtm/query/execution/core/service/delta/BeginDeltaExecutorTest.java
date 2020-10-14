package ru.ibs.dtm.query.execution.core.service.delta;

import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.delta.QueryDeltaResult;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.DeltaQueryResultFactoryImpl;
import ru.ibs.dtm.query.execution.core.service.delta.impl.BeginDeltaExecutor;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.BeginDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BeginDeltaExecutorTest {

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
        delta.setDatamartMnemonic(datamart);
        statusDate = "2020-06-15T05:06:55";
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void executeSuccess() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        QueryDeltaResult res = new QueryDeltaResult(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME), 2L);

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(null);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(new JsonArray());
        queryResult.getResult().add(JsonObject.mapFrom(res));

        when(deltaServiceDao.writeNewDeltaHot(eq(datamart), any()))
                .thenReturn(Future.succeededFuture(2L));

        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryResult);

        beginDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(res.getSinId(), ((QueryResult) promise.future().result()).getResult().getJsonObject(0).getLong("sinId"));
    }

    @Test
    void executeWriteNewDeltaHotError() {
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(null);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        RuntimeException exception = new RuntimeException("write new delta hot error");

        when(deltaServiceDao.writeNewDeltaHot(eq(datamart), any())).thenReturn(Future.failedFuture(exception));

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
    void executeWithNumSuccess() {
        req.setSql("BEGIN DELTA SET 2");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        QueryDeltaResult res = new QueryDeltaResult(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME), 2L);

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(2L);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(new JsonArray());
        queryResult.getResult().add(JsonObject.mapFrom(res));

        when(deltaServiceDao.writeNewDeltaHot(eq(datamart), any())).thenReturn(Future.succeededFuture(2L));

        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryResult);

        beginDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(res.getSinId(), ((QueryResult) promise.future().result()).getResult().getJsonObject(0).getLong("sinId"));
    }

    @Test
    void executeWithNumWriteNewDeltaHotError() {
        req.setSql("BEGIN DELTA SET 2");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(2L);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        RuntimeException exception = new RuntimeException("write new delta hot error");

        when(deltaServiceDao.writeNewDeltaHot(eq(datamart), eq(2L))).thenReturn(Future.failedFuture(exception));

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
    void executeDeltaQueryResultFactoryError(){
        req.setSql("BEGIN DELTA");
        beginDeltaExecutor = new BeginDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        BeginDeltaQuery deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(null);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        Mockito.when(deltaServiceDao.writeNewDeltaHot(eq(datamart), any()))
                .thenReturn(Future.succeededFuture(2L));

        RuntimeException ex = new RuntimeException("delta query result factory error");
        when(deltaQueryResultFactory.create(any(), any())).thenThrow(ex);

        beginDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertNotNull(promise.future().cause());
        assertEquals(ex.getMessage(), promise.future().cause().getMessage());
    }
}
