package ru.ibs.dtm.query.calcite.core.service;

import ru.ibs.dtm.common.model.ddl.Entity;

public interface HSQLQueryService {
    String generateCreateTableQuery(Entity entity);

    String generateCreateViewQuery(Entity entity);
}
