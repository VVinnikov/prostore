package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;

public interface AdbMppwDataTransferService {

    void execute(MppwTransferDataRequest dataRequest, Handler<AsyncResult<Void>> asyncHandler);
}
