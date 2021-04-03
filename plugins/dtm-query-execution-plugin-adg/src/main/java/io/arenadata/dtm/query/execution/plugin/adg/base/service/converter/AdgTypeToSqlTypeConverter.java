package io.arenadata.dtm.query.execution.plugin.adg.base.service.converter;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;

@Component("adgTypeToSqlTypeConverter")
public class AdgTypeToSqlTypeConverter implements SqlTypeConverter {

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return Collections.emptyMap();
    }

    @Override
    public Object convert(ColumnType type, Object value) {
        return value;
    }
}
