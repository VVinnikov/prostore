package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public class FloatFromFloatTransformer implements ColumnTransformer<Float, Float> {

    @Override
    public Float transform(Float value) {
        return value;
    }

    @Override
    public Class<?> getTransformClass() {
        return Float.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.FLOAT;
    }
}
