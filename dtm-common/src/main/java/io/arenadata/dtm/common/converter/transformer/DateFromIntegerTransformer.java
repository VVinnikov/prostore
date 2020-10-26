package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.sql.Date;
import java.time.LocalDate;

public class DateFromIntegerTransformer implements ColumnTransformer<Date, Integer> {

    @Override
    public Date transform(Integer value) {
        return Date.valueOf(LocalDate.ofEpochDay(value.longValue()));
    }

    @Override
    public Class<?> getTransformClass() {
        return Integer.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
