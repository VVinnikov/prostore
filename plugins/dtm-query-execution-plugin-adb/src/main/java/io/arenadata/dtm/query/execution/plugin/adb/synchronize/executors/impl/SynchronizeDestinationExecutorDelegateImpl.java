package io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.impl;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutorDelegate;
import io.arenadata.dtm.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

@Service
public class SynchronizeDestinationExecutorDelegateImpl implements SynchronizeDestinationExecutorDelegate {
    private final Map<SourceType, SynchronizeDestinationExecutor> executorMap;

    public SynchronizeDestinationExecutorDelegateImpl(List<SynchronizeDestinationExecutor> synchronizeDestinationExecutors) {
        executorMap = new HashMap<>(synchronizeDestinationExecutors.size());
        for (SynchronizeDestinationExecutor executor : synchronizeDestinationExecutors) {
            SynchronizeDestinationExecutor previous = executorMap.put(executor.getDestination(), executor);
            if (previous != null) {
                throw new IllegalArgumentException(format("SynchronizeDestinationExecutor of %s already exists, exist: %s, new: %s",
                        executor.getDestination(), previous.getClass().getSimpleName(), executor.getClass().getSimpleName()));
            }
        }
    }

    @Override
    public Future<Long> execute(SourceType sourceType, SynchronizeRequest synchronizeRequest) {
        if (!executorMap.containsKey(sourceType)) {
            return Future.failedFuture(new SynchronizeDatasourceException(format("Synchronize[ADB->%s] is not implemented", sourceType)));
        }

        return executorMap.get(sourceType).execute(synchronizeRequest);
    }
}
