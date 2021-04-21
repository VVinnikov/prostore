package io.arenadata.dtm.query.execution.plugin.adb.base.service.converter;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("adbTypeFromSqlTypeConverter")
public class AdbTypeFromSqlTypeConverter implements SqlTypeConverter {

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    @Autowired
    public AdbTypeFromSqlTypeConverter(@Qualifier("adbFromSqlTransformerMap")
                                                 Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap) {
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }
}