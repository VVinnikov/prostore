package ru.ibs.dtm.query.execution.plugin.adb.factory;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

import java.util.List;

public interface MppwTransferRequestFactory {

    MppwTransferDataRequest create(MppwRequestContext context, List<JsonObject> keyColumns);
}
