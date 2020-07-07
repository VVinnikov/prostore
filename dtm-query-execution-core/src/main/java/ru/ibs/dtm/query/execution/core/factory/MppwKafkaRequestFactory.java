package ru.ibs.dtm.query.execution.core.factory;

import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

public interface MppwKafkaRequestFactory {

    MppwRequestContext create(EdmlRequestContext context);
}
