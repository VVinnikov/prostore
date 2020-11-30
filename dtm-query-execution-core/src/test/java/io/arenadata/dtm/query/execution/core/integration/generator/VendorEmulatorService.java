package io.arenadata.dtm.query.execution.core.integration.generator;

import io.vertx.core.Future;

public interface VendorEmulatorService {

    Future<Void> generateData(String host, int port, Object loadRequest);
}
