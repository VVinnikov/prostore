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
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.DeltaQueryResultFactoryImpl;
import ru.ibs.dtm.query.execution.core.service.delta.impl.CommitDeltaExecutor;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.CommitDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommitDeltaExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(DeltaQueryResultFactoryImpl.class);
    private CommitDeltaExecutor commitDeltaExecutor;
    private QueryRequest req = new QueryRequest();
    private DeltaRecord delta = new DeltaRecord();
    private String datamart = "test_datamart";

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic("test_datamart");
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setLoadId(0L);
        delta.setLoadProcId("load-proc-1");
        delta.setDatamartMnemonic(req.getDatamartMnemonic());
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void executeWriteDeltaHotSuccessError() {
        req.setSql("COMMIT DELTA");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();

        CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
        deltaQuery.setDeltaDateTime(null);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        RuntimeException exception = new RuntimeException("write delta hot success error");

        when(deltaServiceDao.writeDeltaHotSuccess(any(), any())).thenReturn(Future.failedFuture(exception));

        commitDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertEquals(exception, promise.future().cause());
    }

    @Test
    void executeWithDatetimeWriteDeltaHotSuccessError() {
        req.setSql("COMMIT DELTA '2020-06-11T14:00:11'");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();
        String deltaDate = "2020-06-12T18:00:01";

        CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
        deltaQuery.setDeltaDateTime(LocalDateTime.parse(deltaDate));

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        RuntimeException exception = new RuntimeException("write delta hot success error");

        when(deltaServiceDao.writeDeltaHotSuccess(any(), any())).thenReturn(Future.failedFuture(exception));

        commitDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertEquals(exception, promise.future().cause());
    }

    @Test
    void executeSuccess() {
        req.setSql("COMMIT DELTA");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();
        String nowStatusDate = "2020-06-16T14:00:11";

        QueryDeltaResult res = new QueryDeltaResult(nowStatusDate, 1L);

        CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
        deltaQuery.setDeltaDateTime(null);

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(new JsonArray());
        queryDeltaResult.getResult().add(JsonObject.mapFrom(new QueryDeltaResult(nowStatusDate, 1L)));

        when(deltaServiceDao.writeDeltaHotSuccess(any(), any())).thenReturn(Future.succeededFuture(1L));

        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryDeltaResult);

        commitDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(res.getSinId(), ((QueryResult) promise.future().result()).getResult().getJsonObject(0).getLong("sinId"));
        assertEquals(res.getStatusDate(), ((QueryResult) promise.future().result()).getResult().getJsonObject(0).getString("statusDate"));
    }

    @Test
    void executeWithDatetimeSuccess() {
        req.setSql("COMMIT DELTA '2020-06-15T14:00:11'");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();
        String deltaInputDate = "2020-06-15T14:00:11";
        String nowStatusDate = "2020-06-16T14:00:11";

        QueryDeltaResult res = new QueryDeltaResult(nowStatusDate, 1L);

        CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
        deltaQuery.setDeltaDateTime(LocalDateTime.parse(deltaInputDate));

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(new JsonArray());
        queryDeltaResult.getResult().add(JsonObject.mapFrom(new QueryDeltaResult(nowStatusDate, 1L)));

        when(deltaServiceDao.writeDeltaHotSuccess(any(), any())).thenReturn(Future.succeededFuture(1L));

        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryDeltaResult);

        commitDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        assertEquals(res.getSinId(), ((QueryResult) promise.future().result()).getResult().getJsonObject(0).getLong("sinId"));
        assertEquals(res.getStatusDate(), ((QueryResult) promise.future().result()).getResult().getJsonObject(0).getString("statusDate"));
    }

}
