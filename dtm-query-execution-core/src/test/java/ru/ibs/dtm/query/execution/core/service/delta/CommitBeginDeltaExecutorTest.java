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
import ru.ibs.dtm.query.execution.core.service.delta.impl.CommitDeltaExecutor;
import ru.ibs.dtm.query.execution.core.utils.QueryResultUtils;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.CommitDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommitBeginDeltaExecutorTest {

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
    void executeDeltaWithInProgressStatus() {
        req.setSql("COMMIT DELTA '2020-06-11T14:00:11'");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();
        String deltaDate = "2020-06-12T18:00:01";

        CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
        deltaQuery.setDeltaDateTime(LocalDateTime.parse(deltaDate));

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        RuntimeException exception = new RuntimeException("По заданной дельте еще не завершена загрузка данных!");

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(exception));
            return null;
        }).when(deltaServiceDao).getDeltaHotByDatamart(any(), any());

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
    void executeDeltaWithDateTimeBeforeThanDeltaOk() {
        req.setSql("COMMIT DELTA '2020-06-10T14:00:11'");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();
        String deltaDate = "2020-06-15T18:00:01";
        String deltaInputDate = "2020-06-10T14:00:11";

        CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
        deltaQuery.setDeltaDateTime(LocalDateTime.parse(deltaInputDate));

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        RuntimeException exception = new RuntimeException("Заданное время меньше или равно времени актуальной дельты!");

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            delta.setStatus(DeltaLoadStatus.IN_PROCESS);
            delta.setSinId(2L);
            delta.setSysDate(LocalDateTime.parse(deltaDate, DateTimeFormatter.ISO_DATE_TIME));
            handler.handle(Future.succeededFuture(delta));
            return null;
        }).when(deltaServiceDao).getDeltaHotByDatamart(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(2);
            handler.handle(Future.failedFuture(exception));
            return null;
        }).when(deltaServiceDao).getDeltaActualBySinIdAndDatamart(any(), any(), any());

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
    void executeDeltaWithDateTimeAfterDeltaOk() {
        req.setSql("COMMIT DELTA '2020-06-15T14:00:11'");
        commitDeltaExecutor = new CommitDeltaExecutor(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx());
        Promise promise = Promise.promise();
        String deltaInputDate = "2020-06-15T14:00:11";
        String deltaDate = "2020-06-10T18:00:01";
        String nowStatusDate = "2020-06-16T14:00:11";

        QueryDeltaResult res = new QueryDeltaResult(nowStatusDate, 1L);

        CommitDeltaQuery deltaQuery = new CommitDeltaQuery();
        deltaQuery.setDeltaDateTime(LocalDateTime.parse(deltaInputDate));

        DatamartRequest datamartRequest = new DatamartRequest(req);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);

        QueryResult queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(createResult(nowStatusDate, 1L));

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(1);
            delta.setStatus(DeltaLoadStatus.IN_PROCESS);
            delta.setSinId(2L);
            delta.setSysDate(LocalDateTime.parse(deltaDate, DateTimeFormatter.ISO_DATE_TIME));
            handler.handle(Future.succeededFuture(delta));
            return null;
        }).when(deltaServiceDao).getDeltaHotByDatamart(any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<DeltaRecord>> handler = invocation.getArgument(2);
            DeltaRecord deltaRecord = new DeltaRecord();
            deltaRecord.setStatus(DeltaLoadStatus.SUCCESS);
            deltaRecord.setSysDate(LocalDateTime.parse(deltaDate, DateTimeFormatter.ISO_DATE_TIME));
            deltaRecord.setStatusDate(LocalDateTime.parse(nowStatusDate, DateTimeFormatter.ISO_DATE_TIME));
            deltaRecord.setSinId(1L);
            handler.handle(Future.succeededFuture(deltaRecord));
            return null;
        }).when(deltaServiceDao).getDeltaActualBySinIdAndDatamart(any(), any(), any());

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(queryDeltaResult));
            return null;
        }).when(deltaServiceDao).updateDelta(any(), any());

        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryDeltaResult);

        commitDeltaExecutor.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertEquals(res.getSinId(), ((QueryResult) promise.future().result()).getResult().get(0).get("sinId"));
        assertEquals(res.getStatusDate(), ((QueryResult) promise.future().result()).getResult().get(0).get("statusDate"));
    }

    private List<Map<String, Object>> createResult(String statusDate, Long sinId) {
        return QueryResultUtils.createResultWithSingleRow(Arrays.asList("statusDate", "sinId"), Arrays.asList(statusDate, sinId));
    }

}
