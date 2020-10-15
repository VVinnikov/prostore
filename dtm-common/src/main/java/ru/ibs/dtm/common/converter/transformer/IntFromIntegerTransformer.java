package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

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
