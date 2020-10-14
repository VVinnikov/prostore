package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

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
