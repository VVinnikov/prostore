package ru.ibs.dtm.query.execution.core.service.schema;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.dto.DatamartInfo;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.dto.schema.DatamartSchemaKey;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

import java.util.List;
import java.util.Map;

public interface LogicalSchemaService {

    void createSchema(List<DatamartInfo> tableInfoList, Handler<AsyncResult<Map<DatamartSchemaKey, DatamartTable>>> resultHandler);
}
