package ru.ibs.dtm.query.execution.core.converter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.converter.SqlTypeConverter;
import ru.ibs.dtm.common.converter.transformer.ColumnTransformer;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

@Component("coreTypeToSqlTypeConverter")
public class CoreTypeToSqlTypeConverter implements SqlTypeConverter {

    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    @Autowired
    public CoreTypeToSqlTypeConverter(@Qualifier("coreTransformerMap")
                                                  Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap) {
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }

}
