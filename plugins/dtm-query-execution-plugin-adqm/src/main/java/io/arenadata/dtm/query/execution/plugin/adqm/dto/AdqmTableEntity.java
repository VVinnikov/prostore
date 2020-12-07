package io.arenadata.dtm.query.execution.plugin.adqm.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdqmTableEntity {
    private String env;
    private String schema;
    private String name;
    private List<AdqmTableColumn> columns;
    private List<String> sortedKeys;
    private List<String> shardingKeys;

}
