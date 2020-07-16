package ru.ibs.dtm.query.execution.core.service.delta;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import ru.ibs.dtm.query.execution.core.dao.delta.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.impl.DeltaServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.service.impl.DeltaServiceImpl;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeltaServiceImplTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private DeltaService deltaService;

    @BeforeEach
    void setUp() {
        deltaService = new DeltaServiceImpl(serviceDbFacade);
    }

    @Test
    void getDeltasOnDateTimes() {
        Promise promise = Promise.promise();
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        List<ActualDeltaRequest> actualDeltaRequests = Arrays.asList(
                new ActualDeltaRequest("test1", "2019-12-23 15:15:14", false),
                new ActualDeltaRequest("test1", null, true),
                new ActualDeltaRequest("test1", null, true),
                new ActualDeltaRequest("test2", "2019-13-23 15:15:14", false),
                new ActualDeltaRequest("test2", null, true)
        );

        List<Long> deltatList = Arrays.asList(1L, -1L, 1L, 1L, 0L);
        List<Long> deltatResultList = Arrays.asList(1L, 0L, 2L, 1L, 1L);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Long>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(deltatList));
            return null;
        }).when(deltaServiceDao).getDeltasOnDateTimes(eq(actualDeltaRequests), any());

        deltaService.getDeltasOnDateTimes(actualDeltaRequests, ar -> {
            if (ar.succeeded()){
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        assertEquals(deltatResultList, promise.future().result());
    }
}