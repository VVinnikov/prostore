package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

public interface MppwRestLoadRequestFactory {

    RestLoadRequest create(MppwRequestContext context);
}
