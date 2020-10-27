package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

import java.util.List;
import java.util.Map;

public interface MppwTransferRequestFactory {

    MppwTransferDataRequest create(MppwRequestContext context, List<Map<String, Object>> keyColumns);
}