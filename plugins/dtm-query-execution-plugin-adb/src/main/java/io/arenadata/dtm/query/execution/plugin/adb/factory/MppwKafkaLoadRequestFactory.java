package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;

public interface MppwKafkaLoadRequestFactory {

    MppwKafkaLoadRequest create(MppwPluginRequest request, String server, MppwProperties mppwProperties);
}
