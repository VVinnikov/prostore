/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.query.GetDeltaOkQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.delta.BeginDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.impl.GetDeltaOkExecutor;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetDeltaOkExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(BeginDeltaQueryResultFactory.class);
    private DeltaExecutor deltaOkExecutor;
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
        deltaOkExecutor = new GetDeltaOkExecutor(serviceDbFacade, deltaQueryResultFactory);
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaOkQuery deltaQuery = GetDeltaOkQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        final LocalDateTime deltaDate = LocalDateTime.parse("2020-06-15 14:00:11",
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        final long cnFrom = 0L;
        final long deltaNum = 1L;
        OkDelta deltaOk = OkDelta.builder()
                .cnFrom(cnFrom)
                .deltaNum(deltaNum)
                .deltaDate(deltaDate)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum, deltaDate, cnFrom, null));

        when(deltaServiceDao.getDeltaOk(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(deltaQueryResultFactory.create(any()))
                .thenReturn(queryResult);

        deltaOkExecutor.execute(deltaQuery)
                .onComplete(promise);

        assertEquals(deltaNum, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.NUM_FIELD));
        assertEquals(deltaDate, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
        assertEquals(cnFrom, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_FROM_FIELD));
        assertNull(promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_TO_FIELD));
    }

    @Test
    void executeEmptySuccess() {
        deltaOkExecutor = new GetDeltaOkExecutor(serviceDbFacade, deltaQueryResultFactory);
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaOkQuery deltaQuery = GetDeltaOkQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(new ArrayList<>());

        when(deltaServiceDao.getDeltaOk(eq(datamart)))
                .thenReturn(Future.succeededFuture(null));

        when(deltaQueryResultFactory.createEmpty())
                .thenReturn(queryResult);

        deltaOkExecutor.execute(deltaQuery)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeGetDeltaOkError() {
        deltaOkExecutor = new GetDeltaOkExecutor(serviceDbFacade, deltaQueryResultFactory);
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaOkQuery deltaQuery = GetDeltaOkQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        when(deltaServiceDao.getDeltaOk(eq(datamart)))
                .thenReturn(Future.failedFuture(new DtmException("")));

        deltaOkExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        deltaOkExecutor = new GetDeltaOkExecutor(serviceDbFacade, deltaQueryResultFactory);
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaOkQuery deltaQuery = GetDeltaOkQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        final LocalDateTime deltaDate = LocalDateTime.parse("2020-06-15 14:00:11",
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);

        final long cnFrom = 0L;
        final long deltaNum = 1L;
        OkDelta deltaOk = OkDelta.builder()
                .cnFrom(cnFrom)
                .deltaNum(deltaNum)
                .deltaDate(deltaDate)
                .build();

        when(deltaServiceDao.getDeltaOk(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(deltaQueryResultFactory.create(any()))
                .thenReturn(null);

        deltaOkExecutor.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    private List<Map<String, Object>> createResult(long deltaNum, LocalDateTime deltaDate, long cnFrom, Long cnTo) {
        return QueryResultUtils.createResultWithSingleRow(Arrays.asList(
                DeltaQueryUtil.NUM_FIELD,
                DeltaQueryUtil.DATE_TIME_FIELD,
                DeltaQueryUtil.CN_FROM_FIELD,
                DeltaQueryUtil.CN_TO_FIELD),
                Arrays.asList(deltaNum, deltaDate, cnFrom, cnTo));
    }
}
