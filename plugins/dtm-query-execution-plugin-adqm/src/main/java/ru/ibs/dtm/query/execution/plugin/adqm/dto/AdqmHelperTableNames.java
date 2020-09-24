package ru.ibs.dtm.query.execution.plugin.adqm.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Arrays;
import java.util.List;

@Data
@AllArgsConstructor
public class AdqmHelperTableNames {
    private String schema;
    private String actual;
    private String actualShard;

    public List<String> toQualifiedActual() {
        return toQualifiedName(actual);
    }

    public List<String> toQualifiedActualShard() {
        return toQualifiedName(actualShard);
    }

    private List<String> toQualifiedName(String tableName) {
        return Arrays.asList(schema, tableName);
    }
}
