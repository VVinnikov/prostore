package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface AdbMppwDataTransferService {

    void execute(MppwTransferDataRequest dataRequest, Handler<AsyncResult<Void>> asyncHandler);
}
