package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaService;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.dto.delta.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DeltaServiceImplTest {

    private final DeltaQueryParamExtractor deltaQueryParamExtractor = mock(DeltaQueryParamExtractorImpl.class);
    private final DeltaExecutor beginDeltaExecutor = mock(BeginDeltaExecutor.class);
    private DeltaService<QueryResult> deltaService;
    private QueryRequest request = new QueryRequest();

    @BeforeEach
    void setUp() {
        deltaService = new DeltaServiceImpl(deltaQueryParamExtractor, Collections.singletonList(beginDeltaExecutor));
        request.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
    }

    @Test
    void executeWithNullDatamart() {
        Promise promise = Promise.promise();
        DatamartRequest datamartRequest = new DatamartRequest(request);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);

        request.setDatamartMnemonic(null);
        deltaService.execute(context, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertTrue(promise.future().failed());
    }

    @Test
    void executeWithEmptyDatamart() {
        Promise promise = Promise.promise();

        DatamartRequest datamartRequest = new DatamartRequest(request);
        DeltaRequestContext context = new DeltaRequestContext(datamartRequest);
        request.setDatamartMnemonic("");
        deltaService.execute(context, handler -> {
            if (handler.succeeded()) {
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });
        assertTrue(promise.future().failed());
    }
}
