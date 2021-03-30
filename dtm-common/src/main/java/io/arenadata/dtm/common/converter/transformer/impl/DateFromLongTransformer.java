package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Collection;
import java.util.Collections;

public class DateFromLongTransformer extends AbstractColumnTransformer<Integer, Long> {

    @Override
    public Integer transformValue(Long value) {
        return value.intValue();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Long.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
