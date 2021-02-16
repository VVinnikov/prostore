package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;

public class DateFromLocalDateTransformer extends AbstractColumnTransformer<Integer, LocalDate> {

    @Override
    public Integer transformValue(LocalDate value) {
        return value == null ? null : Long.valueOf(value.toEpochDay()).intValue();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(LocalDate.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
