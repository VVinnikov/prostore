package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;

public class TimeFromLocalTimeTransformer extends AbstractColumnTransformer<Long, LocalTime> {

    @Override
    public Long transformValue(LocalTime value) {
        return value == null ? null : value.toSecondOfDay() * 1000L;
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
