package io.arenadata.dtm.query.execution.plugin.adqm.base.configuration;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.converter.transformer.impl.*;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static io.arenadata.dtm.common.converter.transformer.ColumnTransformer.getTransformerMap;

@Configuration
public class ConverterConfiguration {

    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSS[SSS]]";

    @Bean("adqmTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap(DtmConfig dtmSettings) {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, getTransformerMap(new LongFromNumberTransformer()));
        transformerMap.put(ColumnType.VARCHAR, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.CHAR, transformerMap.get(ColumnType.VARCHAR));
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new BigintFromNumberTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new DoubleFromNumberTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new FloatFromNumberTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(new DateFromLongTransformer()));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new TimeFromNumberTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(new TimestampFromStringTransformer(
            DateTimeFormatter.ofPattern(DATE_TIME_FORMAT),
            dtmSettings.getTimeZone()),
            new TimestampFromLongTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer(), new BooleanFromNumericTransformer()));
        transformerMap.put(ColumnType.UUID, getTransformerMap(new UuidFromStringTransformer()));
        transformerMap.put(ColumnType.BLOB, getTransformerMap(new BlobFromObjectTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }

    @Bean("adqmFromSqlTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> adqmFromSqlTransformerMap(DtmConfig dtmSettings) {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, getTransformerMap(new NumberFromLongTransformer()));
        transformerMap.put(ColumnType.VARCHAR, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.CHAR, transformerMap.get(ColumnType.VARCHAR));
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new NumberFromBigintTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new NumberFromDoubleTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new NumberFromFloatTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(new LongDateFromIntTransformer()));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new LongTimeFromLongTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(
                new LongTimestampFromLongTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer(), new BooleanFromNumericTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }
}
