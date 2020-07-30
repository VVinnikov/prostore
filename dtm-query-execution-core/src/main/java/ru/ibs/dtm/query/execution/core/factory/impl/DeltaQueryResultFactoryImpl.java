package ru.ibs.dtm.query.execution.core.factory.impl;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.delta.QueryDeltaResult;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;

@Component
public class DeltaQueryResultFactoryImpl implements DeltaQueryResultFactory {

    public static final String STATUS_DATE_COLUMN = "status_date";
    public static final String SIN_ID_COLUMN = "sin_id";

    @Override
    public QueryResult create(DeltaRequestContext context, DeltaRecord deltaRecord) {
        QueryResult res = new QueryResult();
        res.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        res.setResult(new JsonArray());
        res.getResult().add(JsonObject.mapFrom(new QueryDeltaResult(deltaRecord.getStatusDate().format(DateTimeFormatter.ISO_DATE_TIME),
                deltaRecord.getSinId())));
        res.setMetadata(Arrays.asList(
            new ColumnMetadata(STATUS_DATE_COLUMN, ColumnType.VARCHAR),
            new ColumnMetadata(SIN_ID_COLUMN, ColumnType.VARCHAR)
        ));
        return res;
    }
}
