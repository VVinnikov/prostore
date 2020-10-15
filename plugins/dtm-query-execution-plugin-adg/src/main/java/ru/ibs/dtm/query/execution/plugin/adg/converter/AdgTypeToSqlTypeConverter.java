package ru.ibs.dtm.query.execution.plugin.adg.converter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.converter.SqlTypeConverter;
import ru.ibs.dtm.common.converter.transformer.ColumnTransformer;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.util.Map;

@Component("adgTypeToSqlTypeConverter")
public class AdgTypeToSqlTypeConverter implements SqlTypeConverter {

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    @Autowired
    public AdgTypeToSqlTypeConverter(@Qualifier("adgTransformerMap")
                                             Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap) {
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }
}
