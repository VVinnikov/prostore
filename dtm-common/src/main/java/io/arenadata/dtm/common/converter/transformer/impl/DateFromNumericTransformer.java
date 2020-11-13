package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;

public class DateFromNumericTransformer extends AbstractColumnTransformer<Date, Number> {

    @Override
    public Date transformValue(Number value) {
        return Date.valueOf(LocalDate.ofEpochDay(value.longValue()));
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Long.class, Integer.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
