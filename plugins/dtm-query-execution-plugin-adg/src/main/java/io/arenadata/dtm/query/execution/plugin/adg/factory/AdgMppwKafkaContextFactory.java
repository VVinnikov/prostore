package io.arenadata.dtm.query.execution.plugin.adg.factory;

import io.arenadata.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;

public interface AdgMppwKafkaContextFactory {
    AdgMppwKafkaContext create(MppwRequest request);
}
