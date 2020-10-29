package io.arenadata.dtm.query.execution.core.service;

import org.apache.calcite.sql.SqlCall;
import io.arenadata.dtm.common.model.ddl.Entity;

import java.util.Map;

public interface InformationSchemaService {

    void update(SqlCall sql);

    void initialize();

    Map<String, Entity> getEntities();
}
