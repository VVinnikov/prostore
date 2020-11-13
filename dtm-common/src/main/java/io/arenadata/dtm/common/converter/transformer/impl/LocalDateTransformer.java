package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;

public class LocalDateTransformer extends AbstractColumnTransformer<LocalDate, LocalDate> {

    @Override
    public LocalDate transformValue(LocalDate value) {
        return value;
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
