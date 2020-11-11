package io.arenadata.dtm.query.execution.core.service;

import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

public interface InformationSchemaService {

    Future<Void> update(SqlNode sql);

    Future<Void> createInformationSchemaViews();
}
