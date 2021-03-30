package io.arenadata.dtm.query.execution.plugin.adg.converter;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("adgTypeFromSqlTypeConverter")
public class AdgTypeFromSqlTypeConverter implements SqlTypeConverter {

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    @Autowired
    public AdgTypeFromSqlTypeConverter(@Qualifier("adgFromSqlTransformerMap")
                                                 Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap) {
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }
}
