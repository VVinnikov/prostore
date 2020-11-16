package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;

public class LocalTimeTransformer extends AbstractColumnTransformer<LocalTime, LocalTime> {

    @Override
    public LocalTime transformValue(LocalTime value) {
        return value;
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(LocalTime.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIME;
    }
}