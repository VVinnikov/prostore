package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

public interface MppwKafkaLoadRequestFactory {

    MppwKafkaLoadRequest create(MppwKafkaRequest request, String server, MppwProperties mppwProperties);
}
