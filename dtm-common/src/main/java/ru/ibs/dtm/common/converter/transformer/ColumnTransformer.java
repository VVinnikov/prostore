package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

public interface ColumnTransformer<T, V> {

    T transform(V value);

    Class<?> getTransformClass();

    ColumnType getType();
}
