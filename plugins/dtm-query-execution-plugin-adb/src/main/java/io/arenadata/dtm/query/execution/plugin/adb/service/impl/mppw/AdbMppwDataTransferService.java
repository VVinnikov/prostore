package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.vertx.core.Future;

public interface AdbMppwDataTransferService {

    Future<Void> execute(MppwTransferDataRequest dataRequest);
}
