package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.model.ddl.Entity;

import java.util.List;

public interface InformationSchemaService {

    void initialize();

    List<Entity> getEntities();
}
