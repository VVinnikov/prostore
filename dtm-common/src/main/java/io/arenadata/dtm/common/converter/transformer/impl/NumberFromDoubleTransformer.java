package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class NumberFromDoubleTransformer extends AbstractColumnTransformer<Number, Number> {

    @Override
    public Number transformValue(Number value) {
        return value;
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Double.class,
                Float.class,
                Long.class,
                Integer.class,
                BigDecimal.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DOUBLE;
    }
}
