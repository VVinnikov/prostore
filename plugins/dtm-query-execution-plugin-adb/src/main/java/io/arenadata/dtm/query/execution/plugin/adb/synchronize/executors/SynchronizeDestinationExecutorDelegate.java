package io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;

public interface SynchronizeDestinationExecutorDelegate {
    Future<Long> execute(SourceType sourceType, SynchronizeRequest synchronizeRequest);
}
