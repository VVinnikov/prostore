package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

public class UuidFromStringTransformer extends AbstractColumnTransformer<UUID, String> {

    @Override
    public UUID transformValue(String value) {
        return value == null ? null : UUID.fromString(value);
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(String.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.UUID;
    }
}
