package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public class DoubleFromDoubleTransformer implements ColumnTransformer<Double, Double> {

    @Override
    public Double transform(Double value) {
        return value;
    }

    @Override
    public Class<?> getTransformClass() {
        return Double.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DOUBLE;
    }
}
