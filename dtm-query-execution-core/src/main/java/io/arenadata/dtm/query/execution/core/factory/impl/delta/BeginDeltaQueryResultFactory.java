package io.arenadata.dtm.query.execution.core.factory.impl.delta;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component("beginDeltaQueryResultFactory")
public class BeginDeltaQueryResultFactory implements DeltaQueryResultFactory {

    private final SqlTypeConverter converter;

    @Autowired
    public BeginDeltaQueryResultFactory(@Qualifier("coreTypeToSqlTypeConverter") SqlTypeConverter converter) {
        this.converter = converter;
    }

    @Override
    public QueryResult create(DeltaRecord deltaRecord) {
        QueryResult res = new QueryResult();
        res.setResult(new ArrayList<>());
        res.setMetadata(Collections.singletonList(new ColumnMetadata(DeltaQueryUtil.NUM_FIELD, ColumnType.BIGINT)));
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(DeltaQueryUtil.NUM_FIELD, converter.convert(res.getMetadata().get(0).getType(),
                deltaRecord.getDeltaNum()));
        res.getResult().add(rowMap);
        return res;
    }
}
