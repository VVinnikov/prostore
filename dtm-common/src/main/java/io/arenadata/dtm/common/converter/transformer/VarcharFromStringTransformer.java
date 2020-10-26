package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public class VarcharFromStringTransformer implements ColumnTransformer<String, Object> {

    @Override
    public String transform(Object value) {
        return value == null ? null : value.toString();
    }

    @Override
    public Class<?> getTransformClass() {
        return String.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.VARCHAR;
    }
}
