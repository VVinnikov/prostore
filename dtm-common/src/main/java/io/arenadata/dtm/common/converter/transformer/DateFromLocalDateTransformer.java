package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.sql.Date;
import java.time.LocalDate;

public class DateFromLocalDateTransformer implements ColumnTransformer<Date, LocalDate> {

    @Override
    public Date transform(LocalDate value) {
        return value == null ? null : Date.valueOf(value);
    }

    @Override
    public Class<?> getTransformClass() {
        return LocalDate.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
