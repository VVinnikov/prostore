package ru.ibs.dtm.query.execution.core.factory;

import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;

import java.util.List;

public interface MpprKafkaRequestFactory {
    MpprRequestContext create(QueryRequest queryRequest, QueryExloadParam queryExloadParam, List<Datamart> schema);
}
