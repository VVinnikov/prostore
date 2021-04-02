package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTransferDataRequest;
import io.vertx.core.Future;

public interface AdbMppwDataTransferService {

    Future<Void> execute(MppwTransferDataRequest dataRequest);
}
