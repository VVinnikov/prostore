package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class TimestampFromLocalDateTimeTransformer implements ColumnTransformer<Timestamp, LocalDateTime> {

    @Override
    public Timestamp transform(LocalDateTime value) {
        return value == null ? null : Timestamp.valueOf(value);
    }

    @Override
    public Class<?> getTransformClass() {
        return LocalDateTime.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIMESTAMP;
    }
}
