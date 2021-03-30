package io.arenadata.dtm.query.execution.plugin.adb.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdbTableEntity {
    private String name;
    private String schema;
    private List<AdbTableColumn> columns;
    private List<String> primaryKeys;
    private List<String> shardingKeys;
}
