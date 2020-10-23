package io.arenadata.dtm.query.execution.core.factory.impl.delta;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component("beginDeltaQueryResultFactory")
public class BeginDeltaQueryResultFactory implements DeltaQueryResultFactory {

    public static final String DELTA_NUM_COLUMN = "delta_num";

    @Override
    public QueryResult create(DeltaRequestContext context, DeltaRecord deltaRecord) {
        QueryResult res = new QueryResult();
        res.setRequestId(context.getRequest().getQueryRequest().getRequestId());
        res.setResult(new ArrayList<>());
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(DELTA_NUM_COLUMN, deltaRecord.getSinId());
        res.getResult().add(rowMap);
        res.setMetadata(Collections.singletonList(new ColumnMetadata("delta_num", ColumnType.BIGINT)));
        return res;
    }
}
