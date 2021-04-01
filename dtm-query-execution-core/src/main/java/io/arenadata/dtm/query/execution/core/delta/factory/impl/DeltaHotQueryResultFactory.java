package io.arenadata.dtm.query.execution.core.delta.factory.impl;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component("deltaHotQueryResultFactory")
public class DeltaHotQueryResultFactory implements DeltaQueryResultFactory {

    private final SqlTypeConverter converter;

    @Autowired
    public DeltaHotQueryResultFactory(@Qualifier("coreTypeToSqlTypeConverter") SqlTypeConverter converter) {
        this.converter = converter;
    }

    @Override
    public QueryResult create(DeltaRecord deltaRecord) {
        final QueryResult result = createEmpty();
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(DeltaQueryUtil.NUM_FIELD, converter.convert(result.getMetadata().get(0).getType(),
                deltaRecord.getDeltaNum()));
        rowMap.put(DeltaQueryUtil.CN_FROM_FIELD, converter.convert(result.getMetadata().get(1).getType(),
                deltaRecord.getCnFrom()));
        rowMap.put(DeltaQueryUtil.CN_TO_FIELD, converter.convert(result.getMetadata().get(2).getType(),
                deltaRecord.getCnTo()));
        rowMap.put(DeltaQueryUtil.CN_MAX_FIELD, converter.convert(result.getMetadata().get(3).getType(),
                deltaRecord.getCnMax()));
        rowMap.put(DeltaQueryUtil.IS_ROLLING_BACK_FIELD, converter.convert(result.getMetadata().get(4).getType(),
                deltaRecord.isRollingBack()));
        rowMap.put(DeltaQueryUtil.WRITE_OP_FINISHED_FIELD, converter.convert(result.getMetadata().get(5).getType(),
                getWriteOpFinishListString(deltaRecord)));
        result.getResult().add(rowMap);
        return result;
    }

    @Override
    public QueryResult createEmpty() {
        return QueryResult.builder()
            .metadata(getMetadata())
            .build();
    }

    private String getWriteOpFinishListString(DeltaRecord deltaRecord) {
        if (deltaRecord.getWriteOperationsFinished() == null) {
            return null;
        }
        JsonArray wrOpArr = new JsonArray();
        deltaRecord.getWriteOperationsFinished().forEach(wo -> {
            wrOpArr.add(JsonObject.mapFrom(wo));
        });
        return wrOpArr.toString();
    }

    private List<ColumnMetadata> getMetadata() {
        return Arrays.asList(
                new ColumnMetadata(DeltaQueryUtil.NUM_FIELD, ColumnType.BIGINT),
                new ColumnMetadata(DeltaQueryUtil.CN_FROM_FIELD, ColumnType.BIGINT),
                new ColumnMetadata(DeltaQueryUtil.CN_TO_FIELD, ColumnType.BIGINT),
                new ColumnMetadata(DeltaQueryUtil.CN_MAX_FIELD, ColumnType.BIGINT),
                new ColumnMetadata(DeltaQueryUtil.IS_ROLLING_BACK_FIELD, ColumnType.BOOLEAN),
                new ColumnMetadata(DeltaQueryUtil.WRITE_OP_FINISHED_FIELD, ColumnType.VARCHAR)
        );
    }
}
