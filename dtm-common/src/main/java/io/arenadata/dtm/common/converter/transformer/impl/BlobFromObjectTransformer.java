package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Collection;
import java.util.Collections;

public class BlobFromObjectTransformer extends AbstractColumnTransformer<Object, Object> {

    @Override
    public Object transformValue(Object value) {
        return value;
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Object.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.BLOB;
    }
}
