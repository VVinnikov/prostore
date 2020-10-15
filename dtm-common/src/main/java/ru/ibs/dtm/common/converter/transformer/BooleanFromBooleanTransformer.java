package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

public class BooleanFromBooleanTransformer implements ColumnTransformer<Boolean, Boolean> {

    @Override
    public Boolean transform(Boolean value) {
        return value;
    }

    @Override
    public Class<?> getTransformClass() {
        return Boolean.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.BOOLEAN;
    }
}
