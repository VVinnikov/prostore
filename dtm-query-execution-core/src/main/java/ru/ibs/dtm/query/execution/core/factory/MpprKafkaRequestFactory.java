package ru.ibs.dtm.query.execution.core.factory;

import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;

public interface MpprKafkaRequestFactory {
    MpprRequestContext create(EdmlRequestContext context);
}
