package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

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
