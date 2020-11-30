package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

public interface MppwKafkaLoadRequestFactory {

    MppwKafkaLoadRequest create(MppwRequestContext context, String server, MppwProperties mppwProperties);
}
