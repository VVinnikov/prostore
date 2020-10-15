package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.sql.Time;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

public class TimeFromLongTransformer implements ColumnTransformer<Time, Long> {

    @Override
    public Time transform(Long value) {
        return value == null ? null : Time.valueOf(LocalTime.ofNanoOfDay(value));
    }

    @Override
    public Class<?> getTransformClass() {
        return Long.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIME;
    }
}
