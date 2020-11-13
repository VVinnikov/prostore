package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

public class LongFromNumberTransformer extends AbstractColumnTransformer<Long, Number> {

    @Override
    public Long transformValue(Number value) {
        return value.longValue();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Long.class, Integer.class, BigInteger.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.INT;
    }
}
