package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;

public interface MppwRequestFactory<T> {

    T create(MppwTransferDataRequest request);
}
