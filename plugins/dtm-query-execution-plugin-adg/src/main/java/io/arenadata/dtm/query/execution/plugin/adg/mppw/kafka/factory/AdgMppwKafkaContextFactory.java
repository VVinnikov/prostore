package io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.factory;

import io.arenadata.dtm.query.execution.plugin.adg.mppw.kafka.dto.AdgMppwKafkaContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

public interface AdgMppwKafkaContextFactory {
    AdgMppwKafkaContext create(MppwKafkaRequest request);
}
