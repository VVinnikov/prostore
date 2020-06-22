package ru.ibs.dtm.query.execution.core.factory.impl;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.delta.QueryDeltaResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;

import java.time.format.DateTimeFormatter;

@Component
public class DeltaQueryResultFactoryImpl implements DeltaQueryResultFactory {

    @Override
    public QueryResult create(DeltaRequestContext context, DeltaRecord deltaRecord) {
        QueryResult res = new QueryResult();
        res.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        res.setResult(new JsonArray());
        res.getResult().add(JsonObject.mapFrom(new QueryDeltaResult(deltaRecord.getStatusDate().format(DateTimeFormatter.ISO_DATE_TIME),
                deltaRecord.getSinId())));
        return res;
    }
}
