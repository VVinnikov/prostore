package io.arenadata.dtm.query.execution.plugin.adb.service.impl.rollback;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.AdbRollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.query.impl.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.service.AdbRollbackService;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdbRollbackServiceTest {

    private final RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory = mock(AdbRollbackRequestFactory.class);
    private final AdbQueryExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private AdbRollbackService adbRollbackService = new AdbRollbackService(rollbackRequestFactory, adbQueryExecutor);

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        RollbackRequest rollbackRequest = RollbackRequest.builder().build();
        AdbRollbackRequest sqlList = new AdbRollbackRequest(
            PreparedStatementRequest.onlySql("deleteFromHistory"),
            PreparedStatementRequest.onlySql("deleteFromActualSql"),
            PreparedStatementRequest.onlySql("truncateSql"),
            PreparedStatementRequest.onlySql("insertSql")
        );
        when(rollbackRequestFactory.create(any())).thenReturn(sqlList);
        Map<String, Integer> execCount = new HashMap<>();
        sqlList.getStatements().forEach(request -> execCount.put(request.getSql(), 0));

        doAnswer(invocation -> {
            final String sql = invocation.getArgument(0);
            execCount.put(sql, 1);
            return Future.succeededFuture();
        }).when(adbQueryExecutor).executeUpdate(any());

        doAnswer(invocation -> {
            final List<PreparedStatementRequest> requests = invocation.getArgument(0);
            requests.forEach(r -> execCount.put(r.getSql(), 1));
            return Future.succeededFuture();
        }).when(adbQueryExecutor).executeInTransaction(any());

        adbRollbackService.execute(rollbackRequest).onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(execCount.get(sqlList.getStatements().get(0).getSql()), 1);
        assertEquals(execCount.get(sqlList.getStatements().get(1).getSql()), 1);
        assertEquals(execCount.get(sqlList.getStatements().get(2).getSql()), 1);
        assertEquals(execCount.get(sqlList.getStatements().get(3).getSql()), 1);
    }

    @Test
    void executeError() {
        Promise promise = Promise.promise();
        RollbackRequest rollbackRequest = RollbackRequest.builder().build();

        when(rollbackRequestFactory.create(any())).thenReturn(new AdbRollbackRequest(
            PreparedStatementRequest.onlySql("deleteFromHistory"),
            PreparedStatementRequest.onlySql("deleteFromActualSql"),
            PreparedStatementRequest.onlySql("truncateSql"),
            PreparedStatementRequest.onlySql("insertSql")
        ));

        when(adbQueryExecutor.executeUpdate(any())).thenReturn(Future.failedFuture(new DataSourceException("")));

        when(adbQueryExecutor.executeInTransaction(any())).thenReturn(Future.failedFuture(new DataSourceException("")));

        adbRollbackService.execute(rollbackRequest).onComplete(promise);
        assertTrue(promise.future().failed());
    }
}
