package io.arenadata.dtm.query.execution.core.base.service.metadata;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;

public interface InformationSchemaService {

    Future<Void> update(Entity entity, String datamart, SqlKind sqlKind);

    Future<Void> initInformationSchema();
}
