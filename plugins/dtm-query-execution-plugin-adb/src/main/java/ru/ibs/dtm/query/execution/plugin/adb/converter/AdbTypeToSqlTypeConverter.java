package ru.ibs.dtm.query.execution.plugin.adb.converter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.converter.SqlTypeConverter;
import ru.ibs.dtm.common.converter.transformer.*;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.util.Map;

@Component("adbTypeToSqlTypeConverter")
public class AdbTypeToSqlTypeConverter implements SqlTypeConverter {

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    @Autowired
    public AdbTypeToSqlTypeConverter(@Qualifier("adbTransformerMap")
                                                 Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap) {
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }
}
