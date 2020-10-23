package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;

public interface MpprKafkaRequestFactory {
    MpprRequestContext create(EdmlRequestContext context);
}
