package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.sql.Time;
import java.time.LocalTime;

public class TimeFromLocalTimeTransformer implements ColumnTransformer<Time, LocalTime> {

    @Override
    public Time transform(LocalTime value) {
        return value == null ? null : Time.valueOf(value);
    }

    @Override
    public Class<?> getTransformClass() {
        return LocalTime.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIME;
    }
}
