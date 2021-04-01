package io.arenadata.dtm.query.execution.core.delta.factory.impl;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component("commitDeltaQueryResultFactory")
public class CommitDeltaQueryResultFactory implements DeltaQueryResultFactory {

    private final SqlTypeConverter converter;

    @Autowired
    public CommitDeltaQueryResultFactory(@Qualifier("coreTypeToSqlTypeConverter") SqlTypeConverter converter) {
        this.converter = converter;
    }

    @Override
    public QueryResult create(DeltaRecord deltaRecord) {
        final QueryResult result = createEmpty();
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(DeltaQueryUtil.DATE_TIME_FIELD, converter.convert(result.getMetadata().get(0).getType(),
                deltaRecord.getDeltaDate()));
        result.getResult().add(rowMap);
        return result;
    }

    @Override
    public QueryResult createEmpty() {
        return QueryResult.builder()
            .metadata(Collections.singletonList(new ColumnMetadata(DeltaQueryUtil.DATE_TIME_FIELD, ColumnType.TIMESTAMP)))
            .build();
    }
}
