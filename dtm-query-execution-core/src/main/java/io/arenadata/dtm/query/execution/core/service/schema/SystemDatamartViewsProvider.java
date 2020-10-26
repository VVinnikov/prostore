package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.query.execution.core.dto.SystemDatamartView;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;

import java.util.List;

public interface SystemDatamartViewsProvider {
    List<SystemDatamartView> getSystemViews();

    Future<Void> fetchSystemViews();

    List<Datamart> getLogicalSchemaFromSystemViews();
}
