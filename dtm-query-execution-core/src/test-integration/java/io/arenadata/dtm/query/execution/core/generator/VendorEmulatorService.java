package io.arenadata.dtm.query.execution.core.generator;

import io.arenadata.dtm.query.execution.core.dto.UnloadSpecDataRequest;
import io.vertx.core.Future;

public interface VendorEmulatorService {

    Future<Void> generateData(String host, int port, UnloadSpecDataRequest loadRequest);
}
