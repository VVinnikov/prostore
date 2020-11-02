package io.arenadata.dtm.query.execution.core.configuration.converter;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.converter.transformer.*;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
public class ConverterConfiguration {

    @Bean("coreTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap(DtmConfig dtmSettings) {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, Stream.of(new IntFromIntegerTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.VARCHAR, Stream.of(new VarcharFromStringTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.CHAR, transformerMap.get(ColumnType.VARCHAR));
        transformerMap.put(ColumnType.BIGINT, Stream.of(new BigintFromLongTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.DOUBLE, Stream.of(new DoubleFromDoubleTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.FLOAT, Stream.of(new FloatFromFloatTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.DATE, Stream.of(new DateFromLocalDateTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.TIME, Stream.of(new TimeFromLocalTimeTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.TIMESTAMP, Stream.of(
                new TimestampFromStringTransformer(DateTimeFormatter.ISO_LOCAL_DATE_TIME,
                        dtmSettings.getTimeZone()),
                new TimestampFromLocalDateTimeTransformer(dtmSettings.getTimeZone()))
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.BOOLEAN, Stream.of(new BooleanFromBooleanTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.UUID, Stream.of(new UuidFromStringTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.BLOB, Stream.of(new BlobFromObjectTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        transformerMap.put(ColumnType.ANY, Stream.of(new AnyFromObjectTransformer(),
                new AnyFromLocalTimeTransformer())
                .collect(Collectors.toMap(ColumnTransformer::getTransformClass, cl -> cl)));
        return transformerMap;
    }
}
