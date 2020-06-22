package ru.ibs.dtm.common.plugin.exload;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableAttribute {
    private String columnName;
    private String dataType;
    private int orderNum;
}
