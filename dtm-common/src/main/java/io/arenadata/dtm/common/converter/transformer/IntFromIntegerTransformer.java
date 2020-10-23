package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public class IntFromIntegerTransformer implements ColumnTransformer<Integer, Integer> {

    @Override
    public Integer transform(Integer value) {
        return value;
    }

    @Override
    public Class<?> getTransformClass() {
        return Integer.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.INT;
    }
}
