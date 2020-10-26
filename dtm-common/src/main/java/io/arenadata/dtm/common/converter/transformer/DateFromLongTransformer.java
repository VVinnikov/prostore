package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

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
