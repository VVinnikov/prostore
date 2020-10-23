package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.time.LocalTime;

public class AnyFromLocalTimeTransformer implements ColumnTransformer<Long, LocalTime> {

    @Override
    public Long transform(LocalTime value) {
        return value == null ? null : value.toNanoOfDay();
    }

    @Override
    public Class<?> getTransformClass() {
        return LocalTime.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.ANY;
    }
}
