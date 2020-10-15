package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

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
