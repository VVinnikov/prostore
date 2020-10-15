package ru.ibs.dtm.query.execution.plugin.adb.factory;

import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

import java.util.List;
import java.util.Map;

public interface MppwTransferRequestFactory {

    MppwTransferDataRequest create(MppwRequestContext context, List<Map<String, Object>> keyColumns);
}
