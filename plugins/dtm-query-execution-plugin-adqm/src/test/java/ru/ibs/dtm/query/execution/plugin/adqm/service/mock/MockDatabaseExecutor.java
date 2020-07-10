package ru.ibs.dtm.query.execution.plugin.adqm.service.mock;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;

import java.util.List;
import java.util.function.Predicate;

public class MockDatabaseExecutor implements DatabaseExecutor {
    private final List<Predicate<String>> expectedCalls;
    private int callCount;

    public MockDatabaseExecutor(List<Predicate<String>> expectedCalls) {
        this.expectedCalls = expectedCalls;
    }

    @Override
    public void execute(String sql, Handler<AsyncResult<JsonArray>> resultHandler) {
        val r = call(sql);
        if (r.getLeft()) {
            resultHandler.handle(Future.succeededFuture());
        } else {
            resultHandler.handle(Future.failedFuture(r.getRight()));
        }
    }

    @Override
    public void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler) {
        val r = call(sql);
        if (r.getLeft()) {
            completionHandler.handle(Future.succeededFuture());
        } else {
            completionHandler.handle(Future.failedFuture(r.getRight()));
        }
    }

    @Override
    public void executeWithParams(String sql, List<Object> params, Handler<AsyncResult<?>> resultHandler) {
        val r = call(sql);
        if (r.getLeft()) {
            resultHandler.handle(Future.succeededFuture());
        } else {
            resultHandler.handle(Future.failedFuture(r.getRight()));
        }
    }

    private Pair<Boolean, String> call(String sql) {
        callCount++;
        if (callCount > expectedCalls.size()) {
            return Pair.of(false, String.format("Extra call. Expected %d, got %d", expectedCalls.size(), callCount));
        }

        Predicate<String> expected = expectedCalls.get(callCount - 1);
        return expected.test(sql) ? Pair.of(true, "")
                : Pair.of(false, String.format("Unexpected SQL: %s", sql));
    }
}
