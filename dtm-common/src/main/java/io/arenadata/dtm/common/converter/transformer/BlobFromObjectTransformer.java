package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public class BlobFromObjectTransformer implements ColumnTransformer<Object, Object> {

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
        return ColumnType.BLOB;
    }
}
