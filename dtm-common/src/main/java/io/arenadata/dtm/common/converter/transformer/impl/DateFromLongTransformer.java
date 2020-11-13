package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;

public class DateFromLongTransformer extends AbstractColumnTransformer<Date, Long> {

    @Override
    public Date transformValue(Long value) {
        return Date.valueOf(LocalDate.ofEpochDay(value));
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Long.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
