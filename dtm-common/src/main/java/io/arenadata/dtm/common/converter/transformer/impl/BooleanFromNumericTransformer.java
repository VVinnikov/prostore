package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

public class BooleanFromNumericTransformer extends AbstractColumnTransformer<Boolean, Number> {

    @Override
    public Boolean transformValue(Number value) {
        return value.intValue() == 1;
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Integer.class, Long.class, BigInteger.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.BOOLEAN;
    }
}
