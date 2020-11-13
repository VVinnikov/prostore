package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.sql.Time;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;

public class TimeFromLongTransformer extends AbstractColumnTransformer<Time, Long> {

    @Override
    public Time transformValue(Long value) {
        return value == null ? null : Time.valueOf(LocalTime.ofNanoOfDay(value));
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
