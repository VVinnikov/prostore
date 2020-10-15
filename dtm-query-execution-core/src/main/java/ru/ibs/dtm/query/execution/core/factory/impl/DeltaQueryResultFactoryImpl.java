package ru.ibs.dtm.query.execution.core.factory.impl;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Component
public class DeltaQueryResultFactoryImpl implements DeltaQueryResultFactory {

    public static final String STATUS_DATE_COLUMN = "status_date";
    public static final String SIN_ID_COLUMN = "sin_id";

    @Override
    public QueryResult create(DeltaRequestContext context, DeltaRecord deltaRecord) {
        QueryResult res = new QueryResult();
        res.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        res.setResult(new ArrayList<>());
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put("statusDate", deltaRecord.getStatusDate().format(DateTimeFormatter.ISO_DATE_TIME));
        rowMap.put("sinId", deltaRecord.getSinId());
        res.getResult().add(rowMap);
        res.setMetadata(Arrays.asList(
                new ColumnMetadata(STATUS_DATE_COLUMN, ColumnType.VARCHAR),
                new ColumnMetadata(SIN_ID_COLUMN, ColumnType.VARCHAR)
        ));
        return res;
    }
}
