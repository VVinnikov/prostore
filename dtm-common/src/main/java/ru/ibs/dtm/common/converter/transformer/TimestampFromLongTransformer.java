package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TimestampFromLongTransformer implements ColumnTransformer<Timestamp, Long> {

    @Override
    public Timestamp transform(Long value) {
        //TODO implement getting ZoneId from configuration
        return value == null ? null : Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.systemDefault()));
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
