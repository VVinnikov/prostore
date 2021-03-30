package io.arenadata.dtm.query.execution.plugin.adg.factory;

import io.arenadata.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

public interface AdgMppwKafkaContextFactory {
    AdgMppwKafkaContext create(MppwKafkaRequest request);
}
