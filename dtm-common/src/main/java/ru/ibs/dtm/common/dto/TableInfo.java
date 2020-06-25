package ru.ibs.dtm.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class TableInfo {
    private String schemaName;
    private String tableName;
}
