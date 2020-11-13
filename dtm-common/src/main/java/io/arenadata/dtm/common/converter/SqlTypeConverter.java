package io.arenadata.dtm.common.converter;

import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Map;

public interface SqlTypeConverter {

    default Object convert(ColumnType type, Object value) {
        if (value == null) {
            return null;
        }
        final Map<Class<?>, ColumnTransformer> transformerClassMap = getTransformerMap().get(type);
        if (transformerClassMap != null && !transformerClassMap.isEmpty()) {
            final ColumnTransformer columnTransformer = transformerClassMap.get(value.getClass());
            if (columnTransformer != null) {
                return columnTransformer.transform(value);
            } else {
                try {
                    return transformerClassMap.get(Object.class).transform(value);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Can't transform value for column type [%s] and class [%s]",
                        type, value.getClass()), e);
                }
            }
        } else {
            throw new RuntimeException(String.format("Can't find transformers for type [%s]", type));
        }
    }

    Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap();
}
