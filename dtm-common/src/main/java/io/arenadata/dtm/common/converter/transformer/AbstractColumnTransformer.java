package io.arenadata.dtm.common.converter.transformer;

public abstract class AbstractColumnTransformer<T, V> implements ColumnTransformer {
    @Override
    @SuppressWarnings("unchecked")
    public Object transform(Object value) {
        return transformValue((V) value);
    }

    public abstract T transformValue(V value);
}
