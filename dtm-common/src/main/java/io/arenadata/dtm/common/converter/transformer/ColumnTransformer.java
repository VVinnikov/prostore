package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

public interface ColumnTransformer<T, V> {

    T transform(V value);

    Class<?> getTransformClass();

    ColumnType getType();
}
