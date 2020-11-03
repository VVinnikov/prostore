package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.time.Instant;

@Data
@NoArgsConstructor
public class TimestampFromLongTransformer implements ColumnTransformer<Timestamp, Long> {

    @Override
    public Timestamp transform(Long value) {
        return value == null ? null : Timestamp.from(Instant.ofEpochMilli(value));
    }

    @Override
    public Class<?> getTransformClass() {
        return Long.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIMESTAMP;
    }
}
