package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

public class FloatFromNumberTransformer extends AbstractColumnTransformer<Float, Number> {

    @Override
    public Float transformValue(Number value) {
        return value.floatValue();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Double.class,
                Float.class,
                Long.class,
                Integer.class,
                BigDecimal.class,
                BigInteger.class,
                Short.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.FLOAT;
    }
}
