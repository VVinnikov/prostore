package ru.ibs.dtm.query.execution.plugin.adb.factory;

import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

public interface MppwRestLoadRequestFactory {

    RestLoadRequest create(MppwRequestContext context);
}
