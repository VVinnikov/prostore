package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

public class AnyFromObjectTransformer implements ColumnTransformer<Object, Object> {

    @Override
    public Object transform(Object value) {
        return value;
    }

    @Override
    public Class<?> getTransformClass() {
        return Object.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.ANY;
    }
}
