package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTransferDataRequest;

public interface MppwRequestFactory<T> {

    T create(MppwTransferDataRequest request);
}
