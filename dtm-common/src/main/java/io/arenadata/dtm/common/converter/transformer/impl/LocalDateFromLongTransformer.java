package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;

public class LocalDateFromLongTransformer extends AbstractColumnTransformer<LocalDate, Long> {

    @Override
    public LocalDate transformValue(Long value) {
        return value == null ? null : LocalDate.ofEpochDay(value);
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
