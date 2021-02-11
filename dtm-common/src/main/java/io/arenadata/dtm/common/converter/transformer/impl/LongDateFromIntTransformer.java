package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;

public class LongDateFromIntTransformer extends AbstractColumnTransformer<Long, Integer> {

    @Override
    public Long transformValue(Integer value) {
        return value.longValue();
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Integer.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
