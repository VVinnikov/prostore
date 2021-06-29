package io.arenadata.dtm.query.execution.plugin.adg.base.configuration;

import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.converter.transformer.impl.*;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

import static io.arenadata.dtm.common.converter.transformer.ColumnTransformer.getTransformerMap;

@Configuration
public class ConverterConfiguration {

    @Bean("adgFromSqlTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> adgFromSqlTransformerMap() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, getTransformerMap(new NumberFromLongTransformer()));
        transformerMap.put(ColumnType.INT32, getTransformerMap(new NumberFromLongTransformer()));
        VarcharFromStringTransformer varcharFromStringTransformer = new VarcharFromStringTransformer();
        transformerMap.put(ColumnType.VARCHAR, getTransformerMap(varcharFromStringTransformer));
        transformerMap.put(ColumnType.CHAR, getTransformerMap(varcharFromStringTransformer));
        transformerMap.put(ColumnType.LINK, getTransformerMap(varcharFromStringTransformer));
        transformerMap.put(ColumnType.UUID, getTransformerMap(varcharFromStringTransformer));
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new NumberFromBigintTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new NumberFromDoubleTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new NumberFromFloatTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(new LongDateFromIntTransformer()));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new LongTimeFromLongTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(
                new LongTimestampFromLongTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }
}
