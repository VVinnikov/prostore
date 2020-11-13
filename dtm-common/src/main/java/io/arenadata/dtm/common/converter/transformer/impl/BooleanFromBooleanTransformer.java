package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Collection;
import java.util.Collections;

public class BooleanFromBooleanTransformer extends AbstractColumnTransformer<Boolean, Boolean> {

    @Override
    public Boolean transformValue(Boolean value) {
        return value;
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Boolean.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.BOOLEAN;
    }
}
