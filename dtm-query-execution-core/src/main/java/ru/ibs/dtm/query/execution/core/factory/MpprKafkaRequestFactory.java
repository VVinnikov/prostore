package ru.ibs.dtm.query.execution.core.factory;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;

public interface MpprKafkaRequestFactory {
	MpprRequestContext create(QueryRequest queryRequest, QueryExloadParam queryExloadParam, JsonObject schema);
}
