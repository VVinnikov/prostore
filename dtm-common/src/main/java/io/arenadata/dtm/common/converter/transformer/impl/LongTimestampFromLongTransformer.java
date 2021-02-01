package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Collection;
import java.util.Collections;

public class LongTimestampFromLongTransformer extends AbstractColumnTransformer<Long, Long> {

    @Override
    public Long transformValue(Long value) {
        return value;
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Long.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIMESTAMP;
    }
}
