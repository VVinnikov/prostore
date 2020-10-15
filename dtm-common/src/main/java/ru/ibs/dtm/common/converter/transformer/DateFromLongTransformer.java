package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.sql.Date;
import java.time.LocalDate;

public class DateFromLongTransformer implements ColumnTransformer<Date, Long> {

    @Override
    public Date transform(Long value) {
        return Date.valueOf(LocalDate.ofEpochDay(value));
    }

    @Override
    public Class<?> getTransformClass() {
        return Long.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
