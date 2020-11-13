package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;

public class LocalTimeFromLongTransformer extends AbstractColumnTransformer<LocalTime, Long> {

    @Override
    public LocalTime transformValue(Long value) {
        return value == null ? null : LocalTime.ofNanoOfDay(value);
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Long.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIME;
    }
}