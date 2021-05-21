package io.arenadata.dtm.query.execution.core.base.service.metadata.impl;

import io.arenadata.dtm.common.reader.InformationSchemaView;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.stream.Collectors;

@Component
@AllArgsConstructor
public class InformationSchemaQueryFactory {

    private final DataTypeMapper dataTypeMapper;

    public String createInitEntitiesQuery() {
        return String.format("SELECT TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, " +
                        selectDataType() +
                        ", IS_NULLABLE" +
                        " FROM information_schema.columns WHERE TABLE_SCHEMA = '%s' and TABLE_NAME in (%s);",
                InformationSchemaView.DTM_SCHEMA_NAME,
                Arrays.stream(InformationSchemaView.values())
                        .map(view -> String.format("'%s'", view.getRealName().toUpperCase()))
                        .collect(Collectors.joining(",")));
    }

    private String selectDataType() {
        StringBuilder result = new StringBuilder();

        result.append(" case ");
        dataTypeMapper.getHsqlToLogicalSchemaMapping().entrySet().forEach(entry -> {
            result.append(String.format(" when DATA_TYPE = '%s' then '%s' ", entry.getKey(), entry.getValue()));
        });
        result.append(" else DATA_TYPE end as DATA_TYPE");

        return result.toString();
    }

}
