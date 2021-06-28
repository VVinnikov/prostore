package io.arenadata.dtm.query.execution.plugin.adb.base.configuration;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.converter.transformer.impl.*;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer.AdbBigintFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer.AdbDoubleFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer.AdbFloatFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer.AdbLongFromNumericTransformer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

import static io.arenadata.dtm.common.converter.transformer.ColumnTransformer.getTransformerMap;

@Configuration
public class ConverterConfiguration {

    @Bean("adbTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap(DtmConfig dtmSettings) {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, getTransformerMap(new AdbLongFromNumericTransformer()));
        transformerMap.put(ColumnType.INT32, getTransformerMap(new AdbLongFromNumericTransformer()));
        transformerMap.put(ColumnType.VARCHAR, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.LINK, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.CHAR, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new AdbBigintFromNumberTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new AdbDoubleFromNumberTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new AdbFloatFromNumberTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(
                new DateFromNumericTransformer(),
                new DateFromLongTransformer(),
                new DateFromLocalDateTransformer()
        ));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new TimeFromLocalTimeTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(new TimestampFromLocalDateTimeTransformer(dtmSettings.getTimeZone())));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer()));
        transformerMap.put(ColumnType.UUID, getTransformerMap(new UuidFromStringTransformer()));
        transformerMap.put(ColumnType.BLOB, getTransformerMap(new BlobFromObjectTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }

    @Bean("adbFromSqlTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> adbFromSqlTransformerMap(DtmConfig dtmSettings) {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, getTransformerMap(new NumberFromLongTransformer()));
        transformerMap.put(ColumnType.INT32, getTransformerMap(new NumberFromLongTransformer()));
        transformerMap.put(ColumnType.VARCHAR, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.CHAR, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.LINK, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.UUID, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new NumberFromBigintTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new NumberFromDoubleTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new NumberFromFloatTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(
                new LocalDateFromIntTransformer(dtmSettings.getTimeZone())
        ));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new LocalTimeFromLongTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(new LocalDateTimeFromLongTransformer(dtmSettings.getTimeZone())));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }
}
