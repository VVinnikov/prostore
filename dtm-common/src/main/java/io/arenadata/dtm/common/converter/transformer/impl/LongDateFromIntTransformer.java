package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

public class LongDateFromIntTransformer extends AbstractColumnTransformer<Number, Number> {

    @Override
    public Number transformValue(Number value) {
        return value.longValue();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Long.class, Integer.class, BigInteger.class, Short.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
