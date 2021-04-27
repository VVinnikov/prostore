package io.arenadata.dtm.query.execution.core.check.service;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.vertx.core.Future;

public interface CheckTableService {

    Future<String> checkEntity(Entity entity, CheckContext context);
}
