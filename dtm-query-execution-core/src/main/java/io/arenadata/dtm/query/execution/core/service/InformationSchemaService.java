package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlCall;

import java.util.Map;

public interface InformationSchemaService {

    void update(SqlCall sql);

    Map<String, Entity> getEntities();

    Future<Void> createInformationSchemaViews();
}
