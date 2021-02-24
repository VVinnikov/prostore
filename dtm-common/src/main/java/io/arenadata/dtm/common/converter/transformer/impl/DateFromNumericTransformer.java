package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Arrays;
import java.util.Collection;

public class DateFromNumericTransformer extends AbstractColumnTransformer<Integer, Number> {

    @Override
    public Integer transformValue(Number value) {
        return value.intValue();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Long.class, Integer.class, Short.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
