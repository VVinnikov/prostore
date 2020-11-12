package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Collection;
import java.util.Collections;

public class VarcharFromStringTransformer extends AbstractColumnTransformer<String, Object> {

    @Override
    public String transformValue(Object value) {
        return value == null ? null : value.toString();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(String.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.VARCHAR;
    }
}
