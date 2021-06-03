package io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.impl;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Component;

@Component
public class AdgSynchronizeDestinationExecutor implements SynchronizeDestinationExecutor {
    @Override
    public Future<Long> execute(SynchronizeRequest synchronizeRequest) {
        return Future.failedFuture(new SynchronizeDatasourceException("Not implemented"));
    }

    @Override
    public SourceType getDestination() {
        return SourceType.ADG;
    }
}
