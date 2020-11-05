package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;

public interface InformationSchemaService {

    Future<Void> update(SqlNode sql);

    Map<String, Entity> getEntities();

    Future<Void> createInformationSchemaViews();
}
