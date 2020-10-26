package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public class BigintFromLongTransformer implements ColumnTransformer<Long, Long> {

    @Override
    public Long transform(Long value) {
        return value;
    }

    @Override
    public Class<?> getTransformClass() {
        return Long.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.BIGINT;
    }
}
