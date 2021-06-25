package io.arenadata.dtm.query.execution.plugin.api.service.shared.adg;

import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedPrepareStagingRequest;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedProperties;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedTransferDataRequest;
import io.vertx.core.Future;

public interface AdgSharedService {
    Future<Void> prepareStaging(AdgSharedPrepareStagingRequest request);

    Future<Void> transferData(AdgSharedTransferDataRequest request);

    AdgSharedProperties getSharedProperties();
}
