package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.model.ddl.Entity;

import java.util.Map;

public interface InformationSchemaService {

    void initialize();

    Map<String, Entity> getEntities();
}
