package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

public class BigintFromIntegerTransformer implements ColumnTransformer<Long, Integer> {

    @Override
    public Long transform(Integer value) {
        return value.longValue();
    }

    @Override
    public Class<?> getTransformClass() {
        return Integer.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.BIGINT;
    }
}
