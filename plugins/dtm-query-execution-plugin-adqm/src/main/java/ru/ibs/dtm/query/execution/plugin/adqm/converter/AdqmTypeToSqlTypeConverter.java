package ru.ibs.dtm.query.execution.plugin.adqm.converter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.converter.SqlTypeConverter;
import ru.ibs.dtm.common.converter.transformer.ColumnTransformer;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.util.Map;

@Component("adqmTypeToSqlTypeConverter")
public class AdqmTypeToSqlTypeConverter implements SqlTypeConverter {

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    @Autowired
    public AdqmTypeToSqlTypeConverter(@Qualifier("adqmTransformerMap")
                                              Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap) {
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }

}
