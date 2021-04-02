package io.arenadata.dtm.query.execution.core.delta;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.operation.WriteOpFinish;
import io.arenadata.dtm.query.execution.core.delta.dto.query.GetDeltaHotQuery;
import io.arenadata.dtm.query.execution.core.delta.service.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.factory.impl.BeginDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.service.impl.GetDeltaHotExecutor;
import io.arenadata.dtm.query.execution.core.delta.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetDeltaHotExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(BeginDeltaQueryResultFactory.class);
    private DeltaExecutor deltaHotExecutor;
    private final QueryRequest req = new QueryRequest();
    private String datamart;

    @BeforeEach
    void setUp() {
        datamart = "test_datamart";
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void executeSuccess() {
        deltaHotExecutor = new GetDeltaHotExecutor(serviceDbFacade, deltaQueryResultFactory);
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaHotQuery deltaQuery = GetDeltaHotQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        final long cnFrom = 0L;
        final long deltaNum = 1L;
        final long cnMax = 10L;
        final boolean isRollingBack = false;
        final List<WriteOpFinish> writeOpFinishList = new ArrayList<>();
        writeOpFinishList.add(new WriteOpFinish("t1", Arrays.asList(0L, 1L)));
        writeOpFinishList.add(new WriteOpFinish("t2", Arrays.asList(0L, 1L)));
        JsonArray wrOpArr = new JsonArray();
        writeOpFinishList.forEach(wo -> {
            wrOpArr.add(JsonObject.mapFrom(wo));
        });
        String writeOpFinishListStr = wrOpArr.toString();
        HotDelta deltaHot = HotDelta.builder()
                .cnFrom(cnFrom)
                .cnMax(cnMax)
                .deltaNum(deltaNum)
                .rollingBack(isRollingBack)
                .writeOperationsFinished(writeOpFinishList)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum, cnFrom, null, cnMax, isRollingBack, writeOpFinishListStr));

        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaHot));

        when(deltaQueryResultFactory.create(any()))
                .thenReturn(queryResult);

        deltaHotExecutor.execute(deltaQuery)
                .onComplete(promise);

        assertEquals(deltaNum, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.NUM_FIELD));
        assertEquals(cnFrom, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_FROM_FIELD));
        assertNull(promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_TO_FIELD));
        assertEquals(cnMax, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_MAX_FIELD));
        assertEquals(isRollingBack, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.IS_ROLLING_BACK_FIELD));
        assertEquals(writeOpFinishListStr, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.WRITE_OP_FINISHED_FIELD));
    }

    @Test
    void executeEmptySuccess() {
        deltaHotExecutor = new GetDeltaHotExecutor(serviceDbFacade, deltaQueryResultFactory);
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaHotQuery deltaQuery = GetDeltaHotQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(new ArrayList<>());

        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(null));

        when(deltaQueryResultFactory.createEmpty())
                .thenReturn(queryResult);

        deltaHotExecutor.execute(deltaQuery)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeGetDeltaHotError() {
        deltaHotExecutor = new GetDeltaHotExecutor(serviceDbFacade, deltaQueryResultFactory);
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaHotQuery deltaQuery = GetDeltaHotQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.failedFuture(new DtmException("")));

        deltaHotExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        deltaHotExecutor = new GetDeltaHotExecutor(serviceDbFacade, deltaQueryResultFactory);
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaHotQuery deltaQuery = GetDeltaHotQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        final long cnFrom = 0L;
        final long deltaNum = 1L;
        final long cnMax = 10L;
        final boolean isRollingBack = false;
        final List<WriteOpFinish> writeOpFinishList = new ArrayList<>();
        writeOpFinishList.add(new WriteOpFinish("t1", Arrays.asList(0L, 1L)));
        writeOpFinishList.add(new WriteOpFinish("t2", Arrays.asList(0L, 1L)));

        HotDelta deltaHot = HotDelta.builder()
                .cnFrom(cnFrom)
                .cnMax(cnMax)
                .deltaNum(deltaNum)
                .rollingBack(isRollingBack)
                .writeOperationsFinished(writeOpFinishList)
                .build();

        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaHot));

        when(deltaQueryResultFactory.create(any()))
                .thenThrow(new DtmException(""));

        deltaHotExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    private List<Map<String, Object>> createResult(long deltaNum, long cnFrom, Long cnTo, long cnMax,
                                                   boolean isRollingBack, String wrOpList) {
        return QueryResultUtils.createResultWithSingleRow(Arrays.asList(
                DeltaQueryUtil.NUM_FIELD,
                DeltaQueryUtil.CN_FROM_FIELD,
                DeltaQueryUtil.CN_TO_FIELD,
                DeltaQueryUtil.CN_MAX_FIELD,
                DeltaQueryUtil.IS_ROLLING_BACK_FIELD,
                DeltaQueryUtil.WRITE_OP_FINISHED_FIELD),
                Arrays.asList(deltaNum, cnFrom, cnTo, cnMax, isRollingBack, wrOpList));
    }
}
