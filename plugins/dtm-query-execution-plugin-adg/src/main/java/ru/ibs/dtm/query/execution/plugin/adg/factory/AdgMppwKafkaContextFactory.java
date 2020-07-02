package ru.ibs.dtm.query.execution.plugin.adg.factory;

import ru.ibs.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

public interface AdgMppwKafkaContextFactory {
    AdgMppwKafkaContext create(MppwRequest request);
}
