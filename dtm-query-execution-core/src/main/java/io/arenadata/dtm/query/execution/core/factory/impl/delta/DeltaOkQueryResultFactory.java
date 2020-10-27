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

import java.util.*;

@Component("deltaOkQueryResultFactory")
public class DeltaOkQueryResultFactory implements DeltaQueryResultFactory {

    private final SqlTypeConverter converter;

    @Autowired
    public DeltaOkQueryResultFactory(@Qualifier("coreTypeToSqlTypeConverter") SqlTypeConverter converter) {
        this.converter = converter;
    }

    @Override
    public QueryResult create(DeltaRecord deltaRecord) {
        QueryResult res = new QueryResult();
        res.setResult(new ArrayList<>());
        res.setMetadata(getMetadata());
        if (deltaRecord != null) {
            Map<String, Object> rowMap = new HashMap<>();
            rowMap.put(DeltaQueryUtil.NUM_FIELD, converter.convert(res.getMetadata().get(0).getType(),
                    deltaRecord.getDeltaNum()));
            rowMap.put(DeltaQueryUtil.DATE_TIME_FIELD, converter.convert(res.getMetadata().get(1).getType(),
                    deltaRecord.getDeltaDate()));
            rowMap.put(DeltaQueryUtil.CN_FROM_FIELD, converter.convert(res.getMetadata().get(2).getType(),
                    deltaRecord.getCnFrom()));
            rowMap.put(DeltaQueryUtil.CN_TO_FIELD, converter.convert(res.getMetadata().get(3).getType(),
                    deltaRecord.getCnTo()));
            res.getResult().add(rowMap);
        }
        return res;
    }

    private List<ColumnMetadata> getMetadata() {
        return Arrays.asList(
                new ColumnMetadata(DeltaQueryUtil.NUM_FIELD, ColumnType.BIGINT),
                new ColumnMetadata(DeltaQueryUtil.DATE_TIME_FIELD, ColumnType.TIMESTAMP),
                new ColumnMetadata(DeltaQueryUtil.CN_FROM_FIELD, ColumnType.BIGINT),
                new ColumnMetadata(DeltaQueryUtil.CN_TO_FIELD, ColumnType.BIGINT)
        );
    }
}
