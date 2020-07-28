package ru.ibs.dtm.query.execution.core.service.schema;

import io.vertx.core.Future;
import ru.ibs.dtm.query.execution.core.dto.SystemDatamartView;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

public interface SystemDatamartViewsProvider {
    List<SystemDatamartView> getSystemViews();

    Future<Void> fetchSystemViews();

    List<Datamart> getLogicalSchemaFromSystemViews();
}
