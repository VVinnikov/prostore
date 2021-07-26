package io.arenadata.dtm.query.execution.core.delta;

import io.arenadata.dtm.query.execution.core.base.configuration.properties.RollbackDeltaProperties;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.service.impl.BreakMppwExecutor;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.BreakMppwContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class BreakMppwExecutorTest {

    private static final String DATAMART = "test_datamart";

    private RollbackDeltaProperties rollbackDeltaProperties;

    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private DeltaServiceDao deltaServiceDao;

    private BreakMppwExecutor executor;

    @BeforeEach
    public void setUp(Vertx vertx) {
        MockitoAnnotations.initMocks(this);

        DeltaWriteOp op1 = new DeltaWriteOp();
        op1.setStatus(WriteOperationStatus.EXECUTING.getValue());
        op1.setSysCn(1L);

        DeltaWriteOp op2 = new DeltaWriteOp();
        op2.setStatus(WriteOperationStatus.EXECUTING.getValue());
        op2.setSysCn(2L);

        DeltaWriteOp op3 = new DeltaWriteOp();
        op3.setStatus(WriteOperationStatus.SUCCESS.getValue());
        op3.setSysCn(3L);

        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(deltaServiceDao.getDeltaWriteOperations(DATAMART)).thenReturn(
                Future.succeededFuture(Arrays.asList(
                        op1, op2, op3
                ))
        );

        rollbackDeltaProperties = new RollbackDeltaProperties();
        rollbackDeltaProperties.setRollbackStatusCallsMs(100);
        executor = new BreakMppwExecutor(serviceDbFacade, rollbackDeltaProperties, vertx);
    }

    @Test
    public void testBreakMppw(VertxTestContext context, Vertx vertx) {
        vertx.setTimer(1000, handler -> {
            if (BreakMppwContext.getNumberOfTasksByDatamart(DATAMART) != 2) {
                context.failNow("Wrong BREAK_MPPW tasks number in the queue");
            }
            BreakMppwContext.removeTask(DATAMART, 1);
            BreakMppwContext.removeTask(DATAMART, 2);
        });

        executor.breakMppw(DATAMART).onComplete(ar -> {
            if (ar.succeeded()) {
                context.completeNow();
            } else {
                context.failNow(ar.cause());
            }
        });
    }

}
