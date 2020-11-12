package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface ColumnTransformer {

    Object transform(Object value);

    Collection<Class<?>> getTransformClasses();

    ColumnType getType();

    default Map<Class<?>, ColumnTransformer> getTransformerMap() {
        return getTransformClasses().stream()
            .collect(Collectors.toMap(Function.identity(), tc -> this));
    }

    static Map<Class<?>, ColumnTransformer> getTransformerMap(ColumnTransformer... columnTransformers) {
        Map<Class<?>, ColumnTransformer> map = new HashMap<>();
        for (ColumnTransformer transformer : columnTransformers) {
            map.putAll(transformer.getTransformClasses().stream()
                .collect(Collectors.toMap(Function.identity(), tc -> transformer)));
        }
        return map;
    }
}
