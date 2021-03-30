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
package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.dto.delta.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.dto.delta.query.*;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.delta.DeltaQueryFactoryImpl;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.metrics.impl.MetricsServiceImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DeltaServiceImplTest {
    private final MetricsService<RequestMetrics> metricsService = mock(MetricsServiceImpl.class);
    private DeltaService<QueryResult> deltaService;
    private final DeltaQueryFactory deltaQueryFactory = mock(DeltaQueryFactoryImpl.class);
    private final DeltaExecutor beginDeltaExecutor = mock(BeginDeltaExecutor.class);
    private final DeltaExecutor commitDeltaExecutor = mock(CommitDeltaExecutor.class);
    private final DeltaExecutor rollbackDeltaExecutor = mock(RollbackDeltaExecutor.class);
    private final DeltaExecutor getDeltaByDateTimeExecutor = mock(GetDeltaByDateTimeExecutor.class);
    private final DeltaExecutor getDeltaByNumExecutor = mock(GetDeltaByNumExecutor.class);
    private final DeltaExecutor getDeltaByHotExecutor = mock(GetDeltaHotExecutor.class);
    private final DeltaExecutor getDeltaByOkExecutor = mock(GetDeltaOkExecutor.class);
    private final String envName = "test";

    @BeforeEach
    void setUp() {
        List<DeltaExecutor> executors = Arrays.asList(
                beginDeltaExecutor,
                commitDeltaExecutor,
                rollbackDeltaExecutor,
                getDeltaByDateTimeExecutor,
                getDeltaByNumExecutor,
                getDeltaByHotExecutor,
                getDeltaByOkExecutor
        );
        executors.forEach(this::setUpExecutor);
        when(metricsService.sendMetrics(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(metricsService.sendMetrics(any(), any(), any(), any())).thenAnswer(answer -> {
            Handler<AsyncResult<QueryResult>> promise = answer.getArgument(3);
            return (Handler<AsyncResult<QueryResult>>) ar -> promise.handle(Future.succeededFuture(ar.result()));
        });
        deltaService = new DeltaServiceImpl(executors,
                metricsService, deltaQueryFactory);
    }

    void setUpExecutor(DeltaExecutor executor) {
        when(executor.getAction()).thenCallRealMethod();
        when(executor.execute(any())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));
    }

    @Test
    void executeWithNullDatamart() {
        DeltaQuery deltaQuery = new BeginDeltaQuery(new QueryRequest(), null, null, null);
        executeTest(getContext(null), deltaQuery, ar -> assertTrue(ar.failed()));
    }

    @Test
    void executeWithEmptyDatamart() {
        DeltaQuery deltaQuery = new BeginDeltaQuery(new QueryRequest(), null, null, null);
        executeTest(getContext(""), deltaQuery, ar -> assertTrue(ar.failed()));
    }

    @Test
    void checkBeginDelta() {
        DeltaQuery deltaQuery = new BeginDeltaQuery(new QueryRequest(), null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(beginDeltaExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkCommitDelta() {
        DeltaQuery deltaQuery = new CommitDeltaQuery(new QueryRequest(), null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(commitDeltaExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkRollbackDelta() {
        DeltaQuery deltaQuery = new RollbackDeltaQuery(new QueryRequest(), null, null, null,  "test", null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(rollbackDeltaExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkGetDeltaByDateTime() {
        DeltaQuery deltaQuery = new GetDeltaByDateTimeQuery(new QueryRequest(), null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(getDeltaByDateTimeExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkGetDeltaByNum() {
        DeltaQuery deltaQuery = new GetDeltaByNumQuery(new QueryRequest(), null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(getDeltaByNumExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkGetDeltaHot() {
        DeltaQuery deltaQuery = new GetDeltaHotQuery(new QueryRequest(), null, null, null,
                null, null, null, false, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(getDeltaByHotExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkGetDeltaOk() {
        DeltaQuery deltaQuery = new GetDeltaOkQuery(new QueryRequest(), null, null, null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(getDeltaByOkExecutor, times(1)).execute(any());
        });
    }

    void executeTest(DeltaRequestContext context, DeltaQuery deltaQuery, Consumer<AsyncResult<QueryResult>> validate) {
        when(deltaQueryFactory.create(any())).thenReturn(deltaQuery);
        deltaService.execute(context)
                .onComplete(validate::accept);
    }

    private DeltaRequestContext getContext(String datamart) {
        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic(datamart);
        DatamartRequest datamartRequest = new DatamartRequest(request);
        return new DeltaRequestContext(new RequestMetrics(), datamartRequest, envName, null);
    }
}
