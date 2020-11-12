package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;

public class DateFromIntegerTransformer extends AbstractColumnTransformer<Date, Integer> {

    @Override
    public Date transformValue(Integer value) {
        return Date.valueOf(LocalDate.ofEpochDay(value.longValue()));
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Integer.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
